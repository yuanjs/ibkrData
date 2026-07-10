import { useState, useCallback, useEffect, useRef } from 'react'
import { QuoteTable } from '../components/QuoteTable'
import { CandleChart } from '../components/CandleChart'
import { api, futuresApi, type FuturesActiveContract } from '../api/client'
import { useMarketStore } from '../store/marketStore'
import { aggregateCandles, getFuturesDailyAsOf, intervalSeconds, normalizeCandles, type CandleLike } from '../utils/chartData'

const DAILY_CHART_LIMIT = 120
const RECENT_HISTORY_REFRESH_BARS = 30
const RECENT_HISTORY_REFRESH_DELAYS_MS = [3_000, 80_000]
const historyCache = new Map<string, CandleLike[]>()

function cloneCandles(rows: CandleLike[]) {
  return rows.map(row => ({ ...row }))
}

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
  const recentRefreshRequestIdRef = useRef(0)
  const recentRefreshTimersRef = useRef<number[]>([])
  const lastLiveBucketRef = useRef<number | null>(null)

  const getCacheKey = useCallback((sym: string, inv: string, isFutures: boolean) => {
    return `${sym}:${inv}:${isFutures ? 'futures' : 'cash'}`
  }, [])

  const normalizeRows = useCallback((rows: CandleLike[], inv: string, isFutures: boolean) => {
    const chartRows = isFutures && inv !== '1d' ? aggregateCandles(rows, inv) : rows
    return dedupeAndSortCandles(normalizeCandles(chartRows, inv))
  }, [])

  const fetchChartRange = useCallback(async (
    sym: string,
    inv: string,
    isFutures: boolean,
    start: Date,
    rangeEnd: Date,
    asOfBase: Date = rangeEnd,
  ) => {
    if (isFutures) {
      if (inv === '1d') {
        return futuresApi.daily(sym, start.toISOString(), getFuturesDailyAsOf(sym, asOfBase), 'back_adjusted', true, DAILY_CHART_LIMIT)
      }
      return futuresApi.minute(sym, start.toISOString(), rangeEnd.toISOString(), 'active_raw', asOfBase.toISOString())
    }
    return api.get<{ time: string, open: number, high: number, low: number, close: number }[]>(
      `/history/${sym}?start=${start.toISOString()}&end=${rangeEnd.toISOString()}&interval=${inv}`
    )
  }, [])

  const fetchHistory = useCallback(async (sym: string, inv: string, isFutures: boolean) => {
    const requestId = ++historyRequestIdRef.current
    const end = new Date()
    const totalHours = getHistoryLookbackHours(inv)
    const initialHours = Math.min(getInitialHistoryLookbackHours(inv), totalHours)
    const totalStart = new Date(end.getTime() - totalHours * 3600 * 1000)
    const initialStart = new Date(end.getTime() - initialHours * 3600 * 1000)
    const cacheKey = getCacheKey(sym, inv, isFutures)
    const cachedRows = historyCache.get(cacheKey)
    const hasCachedRows = Boolean(cachedRows)

    if (cachedRows) {
      setCandles(cloneCandles(cachedRows))
    } else {
      setCandles([])
    }

    try {
      setError(null)

      // For daily bars, extend end time by 1 day to include bars whose UTC noon
      // timestamp is in the future (e.g., today's post-roll-hour bar gets date_str
      // of tomorrow, with UTC noon as its time field).
      const queryEnd = inv === '1d' ? new Date(end.getTime() + 24 * 3600 * 1000) : end

      const recentRows = await fetchChartRange(sym, inv, isFutures, initialStart, queryEnd, end)
      if (historyRequestIdRef.current !== requestId) return
      const normalizedRecentRows = normalizeRows(recentRows, inv, isFutures)
      historyCache.set(cacheKey, cloneCandles(normalizedRecentRows))
      setCandles(normalizedRecentRows)

      if (totalStart >= initialStart) return

      try {
        const olderRows = await fetchChartRange(sym, inv, isFutures, totalStart, initialStart, end)
        if (historyRequestIdRef.current !== requestId) return
        const normalizedRows = normalizeRows([...olderRows, ...recentRows], inv, isFutures)
        historyCache.set(cacheKey, cloneCandles(normalizedRows))
        setCandles(normalizedRows)
      } catch (e) {
        console.warn('Failed to fetch older chart history:', e)
      }
    } catch (e: any) {
      if (historyRequestIdRef.current !== requestId) return
      setError(e.message)
      if (!hasCachedRows) {
        setCandles([])
      }
    }
  }, [fetchChartRange, getCacheKey, normalizeRows])

  const refreshRecentHistory = useCallback(async (sym: string, inv: string, isFutures: boolean) => {
    const seconds = intervalSeconds(inv)
    if (seconds < 60 || inv === '1d' || inv === '1w') return

    const requestId = ++recentRefreshRequestIdRef.current
    const end = new Date()
    const lookbackMs = Math.max(RECENT_HISTORY_REFRESH_BARS * seconds * 1000, 2 * 3600_000)
    const start = new Date(end.getTime() - lookbackMs)
    const cacheKey = getCacheKey(sym, inv, isFutures)

    try {
      const rows = await fetchChartRange(sym, inv, isFutures, start, end, end)
      if (recentRefreshRequestIdRef.current !== requestId) return
      const normalizedRows = normalizeRows(rows, inv, isFutures)
      if (normalizedRows.length === 0) return

      setCandles(prev => {
        const merged = dedupeAndSortCandles([...prev, ...normalizedRows])
        historyCache.set(cacheKey, cloneCandles(merged))
        return merged
      })
    } catch (e) {
      console.warn('Failed to refresh recent chart history:', e)
    }
  }, [fetchChartRange, getCacheKey, normalizeRows])

  useEffect(() => {
    if (activeSymbol) {
      fetchHistory(activeSymbol, chartInterval, isActiveFutures)
    }
  }, [activeSymbol, fetchHistory, chartInterval, isActiveFutures])

  useEffect(() => {
    lastLiveBucketRef.current = null
    recentRefreshRequestIdRef.current += 1
    for (const timer of recentRefreshTimersRef.current) {
      window.clearTimeout(timer)
    }
    recentRefreshTimersRef.current = []
  }, [activeSymbol, chartInterval, isActiveFutures])

  useEffect(() => {
    if (!activeSymbol || !chartLiveTick?.price) return

    const seconds = intervalSeconds(chartInterval)
    if (seconds < 60 || chartInterval === '1d' || chartInterval === '1w') return

    const tickTimeSec = chartLiveTick.time
      ? Math.floor(new Date(chartLiveTick.time).getTime() / 1000)
      : Math.floor(Date.now() / 1000)
    const liveBucket = tickTimeSec - (tickTimeSec % seconds)
    const previousBucket = lastLiveBucketRef.current
    lastLiveBucketRef.current = liveBucket

    if (previousBucket == null || liveBucket <= previousBucket) return

    const timers = RECENT_HISTORY_REFRESH_DELAYS_MS.map(delayMs => {
      const timer = window.setTimeout(() => {
        recentRefreshTimersRef.current = recentRefreshTimersRef.current.filter(current => current !== timer)
        void refreshRecentHistory(activeSymbol, chartInterval, isActiveFutures)
      }, delayMs)
      return timer
    })
    recentRefreshTimersRef.current.push(...timers)
  }, [activeSymbol, chartInterval, chartLiveTick, isActiveFutures, refreshRecentHistory])

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
