import { useState, useCallback, useEffect } from 'react'
import { QuoteTable } from '../components/QuoteTable'
import { CandleChart } from '../components/CandleChart'
import { api } from '../api/client'
import { useMarketStore } from '../store/marketStore'
import { getSymbolDescription, getSymbolDecimalPlaces } from '../config/productConfig'

export function Monitor() {
  const activeSymbol = useMarketStore(s => s.activeSymbol)
  const setActiveSymbol = useMarketStore(s => s.setActiveSymbol)

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
  const [chartInterval, setChartInterval] = useState('1d')
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
      // Fetch appropriate window for each interval
      let hours = 24
      if (inv.endsWith('s') || inv === '1m') hours = 6
      else if (inv === '1d') hours = 24 * 365
      else if (inv === '1w') hours = 24 * 365 * 2
      else if (inv.endsWith('m')) hours = 48
      else hours = 168

      const start = new Date(end.getTime() - hours * 3600 * 1000)

      const res = await api.get<{ time: string, open: number, high: number, low: number, close: number }[]>(
        `/history/${sym}?start=${start.toISOString()}&end=${end.toISOString()}&interval=${inv}`
      )
      // Ensure precise timestamp alignment for lightweight-charts
      setCandles(res.map((d: any) => {
        let t = Math.floor(new Date(d.time).getTime() / 1000)
        if (inv === '1d') {
          // Force 12:00 UTC for daily bars to match live tick bucketing
          const dt = new Date(t * 1000)
          t = Math.floor(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), 12) / 1000)
        }
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
    setChartInterval('1d')
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
      <div className="w-80 flex-shrink-0 border-r overflow-y-auto hidden md:block" style={{ backgroundColor: 'var(--bg-base)', borderRightColor: 'var(--border)' }}>
        <QuoteTable onSelect={handleSelectSymbol} activeSymbol={activeSymbol} />
      </div>
      <div className="flex-1 flex flex-col min-w-0" style={{ backgroundColor: 'var(--bg-base)' }}>
        <div className="p-3 md:p-4 flex-1 overflow-y-auto">
          {error && (
            <div className="p-3 rounded mb-4 text-sm" style={{
              color: '#ef4444',
              backgroundColor: 'var(--bg-danger-bg)',
            }}>{error}</div>
          )}

          <div className="rounded-lg p-3 md:p-4 mb-4" style={{
            backgroundColor: 'var(--bg-elevated)',
            boxShadow: '0 0 0 1px var(--ring-subtle)',
          }}>
            <div className="flex items-baseline justify-between mb-4">
              <h2 className="text-lg md:text-xl font-bold bg-gradient-to-r from-blue-400 to-cyan-300 bg-clip-text text-transparent truncate mr-2">
                {activeSymbol
                  ? `${activeSymbol}${getSymbolDescription(activeSymbol) ? ' (' + getSymbolDescription(activeSymbol) + ')' : ''}`
                  : '未选择标的'}
              </h2>
              {quote && (
                <div className="text-xl md:text-2xl font-mono tabular-nums" style={{ color: 'var(--text-primary)' }}>
                  {quote.last?.toFixed(getSymbolDecimalPlaces(activeSymbol || undefined))}
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
              <div className="h-64 flex items-center justify-center border border-dashed rounded-lg" style={{
                color: 'var(--text-muted)',
                borderColor: 'var(--border)',
              }}>
                请从{window.innerWidth < 768 ? '上方下拉列表' : '左侧列表'}选择一个标的开始监控
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
