import { Writable } from 'stream'
import { CircularBuffer, getJSON, getRandomString, ONE_SEC_IN_MS, postJSON, wait } from '../handy.ts'
import { Filter } from '../types.ts'
import { MultiConnectionRealTimeFeedBase, PoolingClientBase, RealTimeFeedBase } from './realtimefeed.ts'

const kucoinHttpOptions = {
  timeout: 10 * 1000,
  retry: {
    limit: 10,
    statusCodes: [418, 429, 500, 403],
    maxRetryAfter: 120 * 1000
  }
}

export class KucoinFuturesRealTimeFeed extends MultiConnectionRealTimeFeedBase {
  private _httpURL = 'https://api-futures.kucoin.com/api'

  protected *_getRealTimeFeeds(exchange: string, filters: Filter<string>[], timeoutIntervalMS?: number, onError?: (error: Error) => void) {
    const wsFilters = filters.filter((f) => f.channel !== 'contract/details')

    if (wsFilters.length > 0) {
      yield new KucoinFuturesSingleConnectionRealTimeFeed(exchange, wsFilters, this._httpURL, timeoutIntervalMS, onError)
    }

    const contractDetailsFilters = filters.filter((f) => f.channel === 'contract/details')
    if (contractDetailsFilters.length > 0) {
      yield new KucoinFuturesContractDetailsClient(exchange, this._httpURL)
    }
  }
}

export class KucoinFuturesSingleConnectionRealTimeFeed extends RealTimeFeedBase {
  constructor(
    exchange: string,
    filters: Filter<string>[],
    private readonly _httpURL: string,
    timeoutIntervalMS: number | undefined,
    onError?: (error: Error) => void
  ) {
    super(exchange, filters, timeoutIntervalMS, onError)
  }
  protected wssURL = ''
  private readonly bufferedDepthUpdates = new Map<string, CircularBuffer<number>>()

  protected async getWebSocketUrl() {
    const { data: body } = await postJSON<any>(`${this._httpURL}/v1/bullet-public`, { retry: 3, timeout: 10000 })

    return `${body.data.instanceServers[0].endpoint}?token=${body.data.token}&connectId=${getRandomString()}`
  }

  protected mapToSubscribeMessages(filters: Filter<string>[]): any[] {
    this.bufferedDepthUpdates.clear()
    for (const symbol of filters.find((f) => f.channel === 'contractMarket/level2Snapshot')?.symbols ?? []) {
      this.bufferedDepthUpdates.set(symbol, new CircularBuffer<number>(2000))
    }

    return filters
      .filter((f) => f.channel !== 'contractMarket/level2Snapshot')
      .map((filter) => {
        if (!filter.symbols || filter.symbols.length === 0) {
          throw new Error('KucoinFuturesRealTimeFeed requires explicitly specified symbols when subscribing to live feed')
        }

        return {
          id: getRandomString(),
          type: 'subscribe',
          topic: `/${filter.channel}:${filter.symbols.join(',')}`,
          response: true
        }
      })
  }

  protected async provideManualSnapshots(filters: Filter<string>[], shouldCancel: () => boolean) {
    const depthSnapshotFilter = filters.find((f) => f.channel === 'contractMarket/level2Snapshot')
    if (!depthSnapshotFilter) {
      return
    }

    // The futures REST snapshot briefly trails the WebSocket sequence after subscription.
    await wait(ONE_SEC_IN_MS)
    if (shouldCancel()) {
      return
    }

    this.debug('requesting manual snapshots for: %s', depthSnapshotFilter.symbols)
    for (let symbol of depthSnapshotFilter.symbols!) {
      const data = await this.requestAlignedSnapshot(symbol, shouldCancel)
      if (data === undefined) {
        return
      }

      const snapshot = {
        type: 'message',
        generated: true,
        topic: `/contractMarket/level2Snapshot:${symbol}`,
        subject: 'level2Snapshot',
        ...data
      }

      this.manualSnapshotsBuffer.push(snapshot)
    }

    this.debug('requested manual snapshots successfully for: %s ', depthSnapshotFilter.symbols)
  }

  protected onMessage(message: any) {
    if (message.type !== 'message' || message.topic?.startsWith('/contractMarket/level2:') !== true) {
      return
    }

    this.bufferedDepthUpdates.get(message.topic.split(':')[1])?.append(Number(message.data.sequence))
  }

  // KuCoin's REST order book snapshot can trail the WebSocket level2 stream. Following KuCoin's local order book
  // procedure, request the snapshot again when the first buffered update that is not older than the snapshot
  // is not snapshot sequence + 1, as the updates in between would otherwise be missing.
  private async requestAlignedSnapshot(symbol: string, shouldCancel: () => boolean) {
    const maxAttempts = 5

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (shouldCancel()) {
        return
      }

      const { data } = await getJSON<any>(`${this._httpURL}/v1/level2/snapshot?symbol=${symbol}`, kucoinHttpOptions)

      const snapshotIsStale = await this.waitForSnapshotStaleness(symbol, Number(data.data.sequence), shouldCancel)
      if (shouldCancel()) {
        return
      }

      if (snapshotIsStale === false) {
        this.bufferedDepthUpdates.delete(symbol)
        return data
      }

      this.debug('level2 snapshot for %s with sequence %s is older than buffered level2 updates', symbol, data.data.sequence)
      if (attempt < maxAttempts) {
        await wait(attempt * 500)
      }
    }

    throw new Error(`KucoinFuturesRealTimeFeed could not align level2 snapshot for ${symbol}`)
  }

  private async waitForSnapshotStaleness(symbol: string, sequence: number, shouldCancel: () => boolean) {
    // keep previous behavior if no level2 update arrives for given symbol in reasonable time
    for (let attempt = 0; attempt < 30 && shouldCancel() === false; attempt++) {
      const snapshotIsStale = this.snapshotIsStale(symbol, sequence)
      if (snapshotIsStale !== undefined) {
        return snapshotIsStale
      }

      await wait(100)
    }

    return false
  }

  private snapshotIsStale(symbol: string, sequence: number) {
    // empty book, for example for newly listed instrument
    if (sequence <= 0) {
      return false
    }

    const bufferedUpdates = this.bufferedDepthUpdates.get(symbol)
    if (bufferedUpdates === undefined || bufferedUpdates.count === 0) {
      return undefined
    }

    for (const updateSequence of bufferedUpdates.items()) {
      if (updateSequence <= sequence) {
        continue
      }

      return updateSequence > sequence + 1
    }

    return false
  }

  protected messageIsError(message: any): boolean {
    return message.type === 'error'
  }

  protected sendCustomPing = () => {
    this.send({
      id: new Date().valueOf().toString(),
      type: 'ping'
    })
  }

  protected messageIsHeartbeat(msg: any) {
    return msg.type === 'pong'
  }
}

class KucoinFuturesContractDetailsClient extends PoolingClientBase {
  constructor(
    exchange: string,
    private readonly _httpURL: string
  ) {
    super(exchange, 6)
  }

  protected async poolDataToStream(outputStream: Writable) {
    const { data: body } = await getJSON<any>(`${this._httpURL}/v1/contracts/active`, kucoinHttpOptions)

    for (const instrument of body.data) {
      const openInterestMessage = {
        topic: `/contract/details:${instrument.symbol}`,
        type: 'message',
        subject: 'contractDetails',
        generated: true,
        data: instrument
      }

      if (outputStream.writable) {
        outputStream.write(openInterestMessage)
      }
    }
  }
}
