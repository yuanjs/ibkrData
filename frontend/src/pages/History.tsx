import { useEffect, useState } from 'react'
import { api, futuresApi, type FuturesActiveContract, type SymbolSubscription } from '../api/client'
import { CandleChart } from '../components/CandleChart'
import { aggregateCandles, candlesToCsv, getFuturesDailyAsOf, normalizeCandles, type CandleLike } from '../utils/chartData'

export function History() {
  const [symbol, setSymbol] = useState('')
  const [subscriptions, setSubscriptions] = useState<Record<string, SymbolSubscription>>({})
  const [activeContract, setActiveContract] = useState<FuturesActiveContract | null>(null)
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [interval, setIntervalVal] = useState('1min')
  const [candles, setCandles] = useState<any[]>([])
  const [error, setError] = useState<string | null>(null)

  const isFutures = subscriptions[symbol]?.sec_type === 'FUT'
  const exportCsv = () => {
    if (candles.length === 0 || !symbol) return
    const csv = candlesToCsv(candles, symbol, interval)
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${symbol}_${interval}_${new Date().toISOString().replace(/[:.]/g, '-')}.csv`
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  useEffect(() => {
    api.get<SymbolSubscription[]>('/symbols').then(rows => {
      const next: Record<string, SymbolSubscription> = {}
      rows.forEach(row => { next[row.symbol] = row })
      setSubscriptions(next)
    }).catch(err => console.error('Failed to fetch symbols:', err))
  }, [])

  const quick = (days: number) => {
    const e = new Date()
    const s = new Date(e.getTime() - days * 86400_000)
    setEnd(e.toISOString().slice(0, 16))
    setStart(s.toISOString().slice(0, 16))
  }

  const search = async () => {
    if (!symbol) return
    if (!start || !end) {
      setError('请选择开始和结束时间')
      return
    }
    try {
      setError(null)
      setActiveContract(null)
      let data: CandleLike[]
      if (isFutures) {
        const startIso = new Date(start).toISOString()
        const endIso = end ? new Date(end).toISOString() : new Date().toISOString()
        const asOf = end ? getFuturesDailyAsOf(symbol, new Date(end)) : getFuturesDailyAsOf(symbol)
        const contract = await futuresApi.activeContract(symbol, asOf)
        setActiveContract(contract)
        if (interval === '1d') {
          data = await futuresApi.daily(symbol, startIso, asOf, 'back_adjusted', true)
        } else {
          data = await futuresApi.minute(symbol, startIso, endIso, 'active_raw', asOf)
          data = aggregateCandles(data, interval)
        }
      } else {
        data = await api.get<any[]>(`/history/${symbol}?start=${start}&end=${end}&interval=${interval}`)
      }
      setCandles(normalizeCandles(data, interval))
    } catch (e: any) {
      setError(e.message)
      setCandles([])
    }
  }

  return (
    <div className="p-4 space-y-4">
      <div className="flex flex-wrap gap-2 items-end">
        <input value={symbol} onChange={e => setSymbol(e.target.value.toUpperCase())}
          placeholder="标的 (如 AAPL)"
          className="border rounded px-3 py-1.5 text-sm w-32 outline-none focus:ring-2 focus:ring-blue-500"
          style={{
            backgroundColor: 'var(--bg-surface)',
            color: 'var(--text-primary)',
            borderColor: 'var(--border-darker)',
          }} />
        <input type="datetime-local" value={start} onChange={e => setStart(e.target.value)}
          className="border rounded px-3 py-1.5 text-sm outline-none focus:ring-2 focus:ring-blue-500"
          style={{
            backgroundColor: 'var(--bg-surface)',
            color: 'var(--text-primary)',
            borderColor: 'var(--border-darker)',
          }} />
        <input type="datetime-local" value={end} onChange={e => setEnd(e.target.value)}
          className="border rounded px-3 py-1.5 text-sm outline-none focus:ring-2 focus:ring-blue-500"
          style={{
            backgroundColor: 'var(--bg-surface)',
            color: 'var(--text-primary)',
            borderColor: 'var(--border-darker)',
          }} />
        <select value={interval} onChange={e => setIntervalVal(e.target.value)}
          className="border rounded px-3 py-1.5 text-sm outline-none focus:ring-2 focus:ring-blue-500"
          style={{
            backgroundColor: 'var(--bg-surface)',
            color: 'var(--text-primary)',
            borderColor: 'var(--border-darker)',
          }}>
          {['1s', '1min', '5min', '1h'].map(i => <option key={i}>{i}</option>)}
        </select>
        <div className="flex gap-1">
          {[['今日', 1], ['近7天', 7], ['近30天', 30]].map(([label, days]) => (
            <button key={label as string} onClick={() => quick(days as number)}
              className="px-2 py-1.5 text-xs rounded"
              style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>{label}</button>
          ))}
        </div>
        <button onClick={search} className="px-4 py-1.5 text-sm bg-blue-600 text-white rounded hover:bg-blue-500">查询</button>
        {candles.length > 0 && (
          <button onClick={exportCsv}
            className="px-3 py-1.5 text-sm rounded"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>导出CSV</button>
        )}
      </div>
      {error && (
        <div className="p-3 rounded text-sm" style={{ color: '#d32f2f', backgroundColor: 'var(--bg-danger-bg)' }}>
          {error}
        </div>
      )}
      {isFutures && activeContract && (
        <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs px-1" style={{ color: 'var(--text-secondary)' }}>
          <span className="font-mono font-semibold" style={{ color: 'var(--text-primary)' }}>
            {symbol} {activeContract.local_symbol || activeContract.contract_month || activeContract.con_id}
          </span>
          <span>conId {activeContract.con_id}</span>
          {activeContract.contract_month && <span>{activeContract.contract_month}</span>}
          {activeContract.exchange && <span>{activeContract.exchange}</span>}
        </div>
      )}
      {candles.length > 0 && (
        <CandleChart symbol={symbol} data={candles} interval={interval} onIntervalChange={setIntervalVal} />
      )}
    </div>
  )
}
