import { useState, useCallback, useEffect } from 'react'
import { QuoteTable } from '../components/QuoteTable'
import { CandleChart } from '../components/CandleChart'
import { api } from '../api/client'
import { useMarketStore } from '../store/marketStore'

interface HistoryRow {
  time: string
  open: number
  high: number
  low: number
  close: number
}

export function Monitor() {
  const [activeSymbol, setActiveSymbol] = useState<string | null>(null)
  const quote = useMarketStore(s => activeSymbol ? s.quotes[activeSymbol] : null)
  const lastTick = useMarketStore(s => (s.lastTick?.symbol === activeSymbol) ? s.lastTick : null)

  // Use lastTick if available, fallback to quote.last for chart updates
  const chartLiveTick = lastTick || (quote?.last ? {
    symbol: activeSymbol!,
    price: quote.last,
    time: quote.time,
    size: 0
  } : null)

  // NOTE: named chartInterval/setChartInterval to avoid shadowing window.setInterval
  const [chartInterval, setChartInterval] = useState('1m')
  const [candles, setCandles] = useState<any[]>([])
  const [error, setError] = useState<string | null>(null)

  const initQuotes = useMarketStore(s => s.initQuotes)

  useEffect(() => {
    api.get<any[]>('/symbols').then(data => {
      if (Array.isArray(data)) {
        initQuotes(data.map(s => s.symbol))
      }
    }).catch(err => console.error('Failed to fetch symbols:', err))
  }, [initQuotes])

  const fetchHistory = useCallback(async (sym: string, inv: string) => {
    try {
      setError(null)
      const end = new Date()
      // Fetch 6 hours window for smaller intervals, up to 1 year for 1d ones
      let hours = 24
      if (inv.endsWith('s') || inv === '1m') hours = 6
      else if (inv === '1d') hours = 24 * 365
      else if (inv.endsWith('m')) hours = 48
      else hours = 168

      const start = new Date(end.getTime() - hours * 3600 * 1000)

      const res = await api.get<{ time: string, open: number, high: number, low: number, close: number }[]>(
        `/history/${sym}?start=${start.toISOString()}&end=${end.toISOString()}&interval=${inv}`
      )
      // Ensure precise timestamp alignment for lightweight-charts
      setCandles(res.map((d: any) => {
        // For daily bars, we can use the date part if it's 1d, but numeric timestamp is also fine
        const t = Math.floor(new Date(d.time).getTime() / 1000)
        return { ...d, time: t }
      }))
    } catch (e: any) {
      setError(e.message)
      setCandles([])
    }
  }, [])

  useEffect(() => {
    if (activeSymbol) {
      fetchHistory(activeSymbol, chartInterval)
    }
  }, [activeSymbol, fetchHistory, chartInterval])

  const handleSelectSymbol = (sym: string) => {
    setActiveSymbol(sym)
  }

  const handleIntervalChange = (newInterval: string) => {
    setChartInterval(newInterval)
    if (activeSymbol) {
      fetchHistory(activeSymbol, newInterval)
    }
  }

  const quotes = useMarketStore(s => s.quotes)
  const symbols = Object.keys(quotes)

  return (
    <div className="flex h-screen overflow-hidden">
      <div className="w-80 flex-shrink-0 border-r border-[#1f2937] overflow-y-auto hidden md:block bg-[#0f1117]">
        <QuoteTable onSelect={handleSelectSymbol} activeSymbol={activeSymbol} />
      </div>
      <div className="flex-1 flex flex-col min-w-0 bg-[#0b0e14]">
        <div className="p-3 md:p-4 flex-1 overflow-y-auto">
          {error && <div className="text-red-500 bg-red-900/20 p-3 rounded mb-4 text-sm">{error}</div>}

          {/* Mobile Symbol Selector */}
          <div className="md:hidden mb-3">
            <label className="block text-xs text-gray-500 mb-1 ml-1">选择监控标的</label>
            <select
              value={activeSymbol || ''}
              onChange={(e) => handleSelectSymbol(e.target.value)}
              className="w-full bg-[#151924] text-gray-100 border border-gray-700 rounded-lg px-3 py-2.5 outline-none focus:ring-2 focus:ring-blue-500 appearance-none font-mono"
            >
              <option value="" disabled>-- 请选择标的 --</option>
              {symbols.map(sym => (
                <option key={sym} value={sym}>
                  {sym} {quotes[sym].last ? `(@${quotes[sym].last.toFixed(2)})` : ''}
                </option>
              ))}
            </select>
          </div>

          <div className="bg-[#151924] rounded-lg p-3 md:p-4 mb-4 ring-1 ring-white/5">
            <div className="flex items-baseline justify-between mb-4">
              <h2 className="text-lg md:text-xl font-bold bg-gradient-to-r from-blue-400 to-cyan-300 bg-clip-text text-transparent truncate mr-2">
                {activeSymbol || '未选择标的'}
              </h2>
              {quote && (
                <div className="text-xl md:text-2xl font-mono text-gray-100 tabular-nums">
                  {quote.last?.toFixed(2)}
                </div>
              )}
            </div>
            {activeSymbol ? (
              <CandleChart
                symbol={activeSymbol!}
                data={candles}
                liveTick={chartLiveTick}
                interval={chartInterval}
                onIntervalChange={handleIntervalChange}
              />
            ) : (
              <div className="h-64 flex items-center justify-center text-gray-500 border border-dashed border-gray-700 rounded-lg">
                请从{window.innerWidth < 768 ? '上方下拉列表' : '左侧列表'}选择一个标的开始监控
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
