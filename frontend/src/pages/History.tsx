import { useState } from 'react'
import { api } from '../api/client'
import { CandleChart } from '../components/CandleChart'

export function History() {
  const [symbol, setSymbol] = useState('')
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [interval, setIntervalVal] = useState('1min')
  const [candles, setCandles] = useState<any[]>([])

  const quick = (days: number) => {
    const e = new Date()
    const s = new Date(e.getTime() - days * 86400_000)
    setEnd(e.toISOString().slice(0, 16))
    setStart(s.toISOString().slice(0, 16))
  }

  const search = async () => {
    if (!symbol) return
    const data = await api.get<any[]>(`/history/${symbol}?start=${start}&end=${end}&interval=${interval}`)
    setCandles(data)
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
          <a href={`/api/history/${symbol}/export?start=${start}&end=${end}&interval=${interval}`}
            className="px-3 py-1.5 text-sm rounded"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>导出CSV</a>
        )}
      </div>
      {candles.length > 0 && (
        <CandleChart symbol={symbol} data={candles} interval={interval} onIntervalChange={setIntervalVal} />
      )}
    </div>
  )
}
