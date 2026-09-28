import { test } from 'node:test'
import type { AddressInfo } from 'net'
import { createServer } from 'http'
import { assert } from './assertions.ts'
import type { Filter } from '../dist/types.js'
import { KucoinRealTimeFeed } from '../dist/realtimefeeds/kucoin.js'
import { KucoinFuturesSingleConnectionRealTimeFeed } from '../dist/realtimefeeds/kucoinfutures.js'

class TestKucoinRealTimeFeed extends KucoinRealTimeFeed {
  protected readonly httpURL: string

  constructor(filters: Filter<string>[], httpURL: string) {
    super('kucoin', filters, undefined)
    this.httpURL = httpURL
  }

  map(filters: Filter<string>[]) {
    return this.mapToSubscribeMessages(filters)
  }

  observe(message: any) {
    this.onMessage(message)
  }

  async provideSnapshots(filters: Filter<string>[]) {
    await this.provideManualSnapshots(filters, () => false)
    return this.manualSnapshotsBuffer
  }
}

class TestKucoinFuturesRealTimeFeed extends KucoinFuturesSingleConnectionRealTimeFeed {
  constructor(filters: Filter<string>[], httpURL: string) {
    super('kucoin-futures', filters, httpURL, undefined)
  }

  map(filters: Filter<string>[]) {
    return this.mapToSubscribeMessages(filters)
  }

  observe(message: any) {
    this.onMessage(message)
  }

  async provideSnapshots(filters: Filter<string>[]) {
    await this.provideManualSnapshots(filters, () => false)
    return this.manualSnapshotsBuffer
  }
}

const spotFilters = [
  { channel: 'market/level2', symbols: ['BTC-USDT'] },
  { channel: 'market/level2Snapshot', symbols: ['BTC-USDT'] }
]

test('requests KuCoin level2 snapshot again when it is older than the first buffered WebSocket update', async () => {
  const server = await startSnapshotServer('/v1/market/orderbook/level2_100?symbol=BTC-USDT', [
    spotSnapshot('1636276324232', '100.2'),
    spotSnapshot('1636276324270', '100.3')
  ])
  const feed = new TestKucoinRealTimeFeed(spotFilters, server.url)

  try {
    feed.map(spotFilters)
    feed.observe(spotUpdate(1636276324266, 1636276324268))
    feed.observe(spotUpdate(1636276324269, 1636276324272))

    const snapshots = await feed.provideSnapshots(spotFilters)

    assert.strictEqual(server.requestsCount, 2)
    assert.deepStrictEqual(snapshots, [
      {
        type: 'message',
        generated: true,
        topic: '/market/level2Snapshot:BTC-USDT',
        subject: 'trade.l2Snapshot',
        ...spotSnapshot('1636276324270', '100.3')
      }
    ])
  } finally {
    await server.close()
  }
})

test('keeps KuCoin level2 snapshot when no WebSocket update arrives for the symbol', async () => {
  const server = await startSnapshotServer('/v1/market/orderbook/level2_100?symbol=BTC-USDT', [spotSnapshot('1636276324232', '100.2')])
  const feed = new TestKucoinRealTimeFeed(spotFilters, server.url)

  try {
    feed.map(spotFilters)

    const snapshots = await feed.provideSnapshots(spotFilters)

    assert.strictEqual(server.requestsCount, 1)
    assert.strictEqual(snapshots.length, 1)
    assert.strictEqual(snapshots[0].data.sequence, '1636276324232')
  } finally {
    await server.close()
  }
})

test('requests KuCoin futures level2 snapshot again when it is older than the first buffered WebSocket update', async () => {
  const futuresFilters = [
    { channel: 'contractMarket/level2', symbols: ['XBTUSDTM'] },
    { channel: 'contractMarket/level2Snapshot', symbols: ['XBTUSDTM'] }
  ]
  const server = await startSnapshotServer('/v1/level2/snapshot?symbol=XBTUSDTM', [
    futuresSnapshot(1694868048200),
    futuresSnapshot(1694868048360)
  ])
  const feed = new TestKucoinFuturesRealTimeFeed(futuresFilters, server.url)

  try {
    feed.map(futuresFilters)
    feed.observe(futuresUpdate(1694868048361))

    const snapshots = await feed.provideSnapshots(futuresFilters)

    assert.strictEqual(server.requestsCount, 2)
    assert.deepStrictEqual(snapshots, [
      {
        type: 'message',
        generated: true,
        topic: '/contractMarket/level2Snapshot:XBTUSDTM',
        subject: 'level2Snapshot',
        ...futuresSnapshot(1694868048360)
      }
    ])
  } finally {
    await server.close()
  }
})

function spotSnapshot(sequence: string, bestAsk: string) {
  return {
    code: '200000',
    data: { time: 1636276324400, sequence, bids: [['100.1', '0.5']], asks: [[bestAsk, '1.2']] }
  }
}

function spotUpdate(sequenceStart: number, sequenceEnd: number) {
  return {
    type: 'message',
    topic: '/market/level2:BTC-USDT',
    subject: 'trade.l2update',
    data: {
      changes: { asks: [], bids: [['100.1', '0.6', String(sequenceEnd)]] },
      sequenceEnd,
      sequenceStart,
      symbol: 'BTC-USDT',
      time: 1636276324500
    }
  }
}

function futuresSnapshot(sequence: number) {
  return {
    code: '200000',
    data: { symbol: 'XBTUSDTM', sequence, bids: [[26500.1, 120]], asks: [[26500.2, 80]], ts: 1694868048000000000 }
  }
}

function futuresUpdate(sequence: number) {
  return {
    type: 'message',
    topic: '/contractMarket/level2:XBTUSDTM',
    subject: 'level2',
    data: { sequence, change: '26500.1,buy,121', timestamp: 1694868048100 }
  }
}

async function startSnapshotServer(expectedURL: string, responses: object[]) {
  let requestsCount = 0
  const server = createServer((request, response) => {
    assert.strictEqual(request.url, expectedURL)
    const body = responses[Math.min(requestsCount, responses.length - 1)]
    requestsCount++

    response.writeHead(200, { 'Content-Type': 'application/json' })
    response.end(JSON.stringify(body))
  })

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo

  return {
    url: `http://127.0.0.1:${port}`,
    get requestsCount() {
      return requestsCount
    },
    close: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
  }
}
