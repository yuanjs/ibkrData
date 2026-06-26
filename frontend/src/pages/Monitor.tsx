import { useState, useCallback, useEffect, useRef } from 'react'
import { QuoteTable } from '../components/QuoteTable'
import { CandleChart } from '../components/CandleChart'
import { api, futuresApi, type FuturesActiveContract, type SymbolSubscription } from '../api/client'
import { useMarketStore } from '../store/marketStore'
import { aggregateCandles, getFuturesDailyAsOf, normalizeCandles, type CandleLike } from '../utils/chartData'

function getHistoryLookbackHours(interval: string) {
  if (interval.endsWith('s')) return 6
  if (interval === '1m') return 24 * 30
  if (interval.endsWith('m')) return 24 * 90
  if (interval.endsWith('h')) return 24 * 180
  if (interval === '1d') return 24 * 365
  if (interval === '1w') return 24 * 365 * 2
  return 24
}

function getInitialHistoryLookbackHours(interval: string) {
  if (interval.endsWith('s')) return 6
  if (interval === '1m') return 6
  if (interval.endsWith('m')) return 24 * 7
  if (interval.endsWith('h')) return 24 * 14
  return getHistoryLookbackHours(interval)
}

function dedupeAndSortCandles(rows: CandleLike[]) {
  const byTime = new Map<number, CandleLike>()
  for (const row of rows) {
    byTime.set(Number(row.time), row)
  }
  return Array.from(byTime.values()).sort((a, b) => Number(a.time) - Number(b.time))
}

export function Monitor() {
  const activeSymbol = useMarketStore(s => s.activeSymbol)
  const setActiveSymbol = useMarketStore(s => s.setActiveSymbol)

  const quote = useMarketStore(s => activeSymbol ? s.quotes[activeSymbol] : null)
  const lastTick = useMarketStore(s => (s.lastTick?.symbol === activeSymbol) ? s.lastTick : null)
  const isActiveFutures = useMarketStore(s => s.isFuturesSymbol(activeSymbol))
  const activeRollState = useMarketStore(s => activeSymbol ? s.futuresRollStates[activeSymbol] : undefined)

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
  const [activeContract, setActiveContract] = useState<FuturesActiveContract | null>(null)
  const [error, setError] = useState<string | null>(null)
  const historyRequestIdRef = useRef(0)

  const initQuotes = useMarketStore(s => s.initQuotes)

  useEffect(() => {
    api.get<SymbolSubscription[]>('/symbols').then(data => {
      if (Array.isArray(data)) {
        initQuotes(data)
      }
    }).catch(err => console.error('Failed to fetch symbols:', err))
  }, [initQuotes])

  const fetchHistory = useCallback(async (sym: string, inv: string, isFutures: boolean) => {
    const requestId = ++historyRequestIdRef.current
    const end = new Date()
    const totalHours = getHistoryLookbackHours(inv)
    const initialHours = Math.min(getInitialHistoryLookbackHours(inv), totalHours)
    const totalStart = new Date(end.getTime() - totalHours * 3600 * 1000)
    const initialStart = new Date(end.getTime() - initialHours * 3600 * 1000)

    const fetchRange = async (start: Date, rangeEnd: Date) => {
      if (isFutures) {
        if (inv === '1d') {
          return futuresApi.daily(sym, start.toISOString(), getFuturesDailyAsOf(sym, end), 'back_adjusted', true)
        }
        return futuresApi.minute(sym, start.toISOString(), rangeEnd.toISOString(), 'active_raw', end.toISOString())
      }
      return api.get<{ time: string, open: number, high: number, low: number, close: number }[]>(
        `/history/${sym}?start=${start.toISOString()}&end=${rangeEnd.toISOString()}&interval=${inv}`
      )
    }

    const normalizeRows = (rows: CandleLike[]) => {
      const chartRows = isFutures && inv !== '1d' ? aggregateCandles(rows, inv) : rows
      return dedupeAndSortCandles(normalizeCandles(chartRows, inv))
    }

    try {
      setError(null)

      // For daily bars, extend end time by 1 day to include bars whose UTC noon
      // timestamp is in the future (e.g., today's post-roll-hour bar gets date_str
      // of tomorrow, with UTC noon as its time field).
      const queryEnd = inv === '1d' ? new Date(end.getTime() + 24 * 3600 * 1000) : end

      const recentRows = await fetchRange(initialStart, queryEnd)
      if (historyRequestIdRef.current !== requestId) return
      setCandles(normalizeRows(recentRows))

      if (totalStart >= initialStart) return

      try {
        const olderRows = await fetchRange(totalStart, initialStart)
        if (historyRequestIdRef.current !== requestId) return
        setCandles(normalizeRows([...olderRows, ...recentRows]))
      } catch (e) {
        console.warn('Failed to fetch older chart history:', e)
      }
    } catch (e: any) {
      if (historyRequestIdRef.current !== requestId) return
      setError(e.message)
      setCandles([])
    }
  }, [])

  useEffect(() => {
    if (activeSymbol) {
      fetchHistory(activeSymbol, chartInterval, isActiveFutures)
    }
  }, [activeSymbol, fetchHistory, chartInterval, isActiveFutures, activeRollState?.active?.con_id])

  useEffect(() => {
    if (!activeSymbol || !isActiveFutures) {
      setActiveContract(null)
      return
    }
    let cancelled = false
    futuresApi.activeContract(activeSymbol)
      .then(contract => {
        if (!cancelled) setActiveContract(contract)
      })
      .catch(err => {
        if (!cancelled) console.error('Failed to fetch active futures contract:', err)
      })
    return () => { cancelled = true }
  }, [activeSymbol, isActiveFutures, activeRollState?.active?.con_id])

  useEffect(() => {
    if (activeRollState?.active) {
      setActiveContract(activeRollState.active)
    }
  }, [activeRollState])

  const handleSelectSymbol = (sym: string) => {
    setActiveSymbol(sym)
    setChartInterval('1d')
  }

  const handleIntervalChange = (newInterval: string) => {
    setChartInterval(newInterval)
  }

  const contract = activeRollState?.active || activeContract

  return (
    <div className="flex h-screen overflow-hidden">
      <div className="w-80 flex-shrink-0 border-r overflow-y-auto hidden md:block" style={{ backgroundColor: 'var(--bg-base)', borderRightColor: 'var(--border)' }}>
        <QuoteTable onSelect={handleSelectSymbol} activeSymbol={activeSymbol} />
      </div>
      <div className="flex-1 flex flex-col min-w-0" style={{ backgroundColor: 'var(--bg-base)' }}>
        <div className="p-1 pb-12 md:p-4 flex-1 overflow-y-auto">
          {error && (
            <div className="p-3 rounded mb-4 text-sm" style={{
              color: '#d32f2f',
              backgroundColor: 'var(--bg-danger-bg)',
            }}>{error}</div>
          )}

          <div className="rounded-lg px-1 py-1 md:p-4 mb-2" style={{
            backgroundColor: 'var(--bg-elevated)',
            boxShadow: '0 0 0 1px var(--ring-subtle)',
          }}>
            {activeSymbol && isActiveFutures && contract && (
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-2 pb-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
                <span className="font-mono font-semibold" style={{ color: 'var(--text-primary)' }}>
                  {activeSymbol} {contract.local_symbol || contract.contract_month || contract.con_id}
                </span>
                <span>conId {contract.con_id}</span>
                {contract.contract_month && <span>{contract.contract_month}</span>}
                {contract.exchange && <span>{contract.exchange}</span>}
                {contract.effective_from && <span>from {new Date(contract.effective_from).toLocaleString()}</span>}
              </div>
            )}
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
