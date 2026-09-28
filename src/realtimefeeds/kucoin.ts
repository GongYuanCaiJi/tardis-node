import { CircularBuffer, getRandomString, getJSON, postJSON, wait } from '../handy.ts'
import { Filter } from '../types.ts'
import { RealTimeFeedBase } from './realtimefeed.ts'

export class KucoinRealTimeFeed extends RealTimeFeedBase {
  protected wssURL = ''
  protected readonly httpURL: string = 'https://api.kucoin.com/api'
  private readonly bufferedDepthUpdates = new Map<string, CircularBuffer<KucoinDepthUpdateSequence>>()

  protected async getWebSocketUrl() {
    const { data: body } = await postJSON<any>(`${this.httpURL}/v1/bullet-public`, { retry: 3, timeout: 10000 })

    return `${body.data.instanceServers[0].endpoint}?token=${body.data.token}&connectId=${getRandomString()}`
  }

  protected mapToSubscribeMessages(filters: Filter<string>[]): any[] {
    this.bufferedDepthUpdates.clear()
    for (const symbol of filters.find((f) => f.channel === 'market/level2Snapshot')?.symbols ?? []) {
      this.bufferedDepthUpdates.set(symbol, new CircularBuffer<KucoinDepthUpdateSequence>(2000))
    }

    return filters
      .filter((f) => f.channel !== 'market/level2Snapshot')
      .map((filter) => {
        if (!filter.symbols || filter.symbols.length === 0) {
          throw new Error('KucoinRealTimeFeed requires explicitly specified symbols when subscribing to live feed')
        }

        return {
          id: getRandomString(),
          type: 'subscribe',
          topic: `/${filter.channel}:${filter.symbols.join(',')}`,
          privateChannel: false,
          response: true
        }
      })
  }

  protected onMessage(message: any) {
    if (message.type !== 'message' || message.topic?.startsWith('/market/level2:') !== true) {
      return
    }

    this.bufferedDepthUpdates.get(message.topic.split(':')[1])?.append({
      sequenceStart: Number(message.data.sequenceStart),
      sequenceEnd: Number(message.data.sequenceEnd)
    })
  }

  protected async provideManualSnapshots(filters: Filter<string>[], shouldCancel: () => boolean) {
    const depthSnapshotFilter = filters.find((f) => f.channel === 'market/level2Snapshot')
    if (!depthSnapshotFilter) {
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
        topic: `/market/level2Snapshot:${symbol}`,
        subject: 'trade.l2Snapshot',
        ...data
      }

      this.manualSnapshotsBuffer.push(snapshot)
    }

    this.debug('requested manual snapshots successfully for: %s ', depthSnapshotFilter.symbols)
  }

  // KuCoin's REST order book snapshot can trail the WebSocket level2 stream. Following KuCoin's local order book
  // procedure, request the snapshot again when the first buffered update that is not older than the snapshot
  // starts after snapshot sequence + 1, as the updates in between would otherwise be missing.
  private async requestAlignedSnapshot(symbol: string, shouldCancel: () => boolean) {
    const maxAttempts = 5

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (shouldCancel()) {
        return
      }

      const { data } = await getJSON<any>(`${this.httpURL}/v1/market/orderbook/level2_100?symbol=${symbol}`, {
        timeout: 10000
      })

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

    throw new Error(`KucoinRealTimeFeed could not align level2 snapshot for ${symbol}`)
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

    for (const update of bufferedUpdates.items()) {
      if (update.sequenceEnd <= sequence) {
        continue
      }

      return update.sequenceStart > sequence + 1
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

type KucoinDepthUpdateSequence = {
  sequenceStart: number
  sequenceEnd: number
}
