import { useState, useCallback, useEffect, useRef } from 'react'
import { View, Text, StyleSheet } from 'react-native'
import { CandleChartRN } from '../src/components/CandleChartRN'
import { api, futuresApi, type FuturesActiveContract, type SymbolSubscription } from '../src/api/client'
import { useMarketStore } from '../src/stores/marketStore'
import { useTheme } from '../src/theme'
import { aggregateCandles, getFuturesDailyAsOf, intervalSeconds, normalizeCandles, type CandleLike } from '../src/utils/chartData'

const RECENT_HISTORY_REFRESH_BARS = 30
const RECENT_HISTORY_REFRESH_DELAYS_MS = [3_000, 80_000]
const DAILY_CHART_LIMIT = 120
const DAILY_CHART_PAGE_BARS = 120
const MAX_DAILY_CHART_BARS = 5000
// Daily bars are paged by growing `limit` while keeping `as_of` fixed, so the
// back-adjusted price series stays continuous across pages.
const dailyLimitCache = new Map<string, number>()
const dailyExhausted = new Set<string>()

function getHistoryLookbackHours(interval: string) {
  if (interval.endsWith('s')) return 6
  if (interval === '1m') return 24 * 30
  if (interval.endsWith('m')) return 24 * 90
  if (interval.endsWith('h')) return 24 * 180
  // Daily history is bounded by the request limit, not by the lookback window.
  if (interval === '1d') return 24 * 365 * 10
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

function getDailyPageKey(sym: string, isFutures: boolean) {
  return `${sym}:1d:${isFutures ? 'futures' : 'cash'}`
}

function dedupeAndSortCandles(rows: CandleLike[]) {
  const byTime = new Map<number, CandleLike>()
  for (const row of rows) {
    byTime.set(Number(row.time), row)
  }
  return Array.from(byTime.values()).sort((a, b) => Number(a.time) - Number(b.time))
}

export default function Monitor() {
  const activeSymbol = useMarketStore(s => s.activeSymbol)
  const setActiveSymbol = useMarketStore(s => s.setActiveSymbol)
  const quote = useMarketStore(s => activeSymbol ? s.quotes[activeSymbol] : null)
  const lastTick = useMarketStore(s => (s.lastTick?.symbol === activeSymbol) ? s.lastTick : null)
  const isActiveFutures = useMarketStore(s => s.isFuturesSymbol(activeSymbol))
  const activeRollState = useMarketStore(s => activeSymbol ? s.futuresRollStates[activeSymbol] : undefined)

  const chartLiveTick = lastTick || (quote?.last ? {
    symbol: activeSymbol!,
    price: quote.last,
    time: quote.time,
    size: 0,
  } : null)

  const [chartInterval, setChartInterval] = useState('1d')
  const [candles, setCandles] = useState<any[]>([])
  const [activeContract, setActiveContract] = useState<FuturesActiveContract | null>(null)
  const [error, setError] = useState<string | null>(null)
  const historyRequestIdRef = useRef(0)
  const recentRefreshRequestIdRef = useRef(0)
  const recentRefreshTimersRef = useRef<ReturnType<typeof setTimeout>[]>([])
  const lastLiveBucketRef = useRef<number | null>(null)
  const loadingMoreRef = useRef(false)

  const initQuotes = useMarketStore(s => s.initQuotes)

  useEffect(() => {
    api.get<SymbolSubscription[]>('/symbols').then(data => {
      if (Array.isArray(data)) {
        initQuotes(data)
      }
    }).catch(err => console.error('Failed to fetch symbols:', err))
  }, [initQuotes])

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
    dailyLimit: number = DAILY_CHART_LIMIT,
  ) => {
    if (isFutures) {
      if (inv === '1d') {
        return futuresApi.daily(sym, start.toISOString(), getFuturesDailyAsOf(sym, asOfBase), 'back_adjusted', true, dailyLimit)
      }
      return futuresApi.minute(sym, start.toISOString(), rangeEnd.toISOString(), 'active_raw', asOfBase.toISOString())
    }
    return api.get<{ time: string; open: number; high: number; low: number; close: number }[]>(
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

    try {
      setError(null)
      const queryEnd = inv === '1d' ? new Date(end.getTime() + 24 * 3600 * 1000) : end

      const recentRows = await fetchChartRange(
        sym, inv, isFutures, initialStart, queryEnd, end,
        dailyLimitCache.get(getDailyPageKey(sym, isFutures)) ?? DAILY_CHART_LIMIT,
      )
      if (historyRequestIdRef.current !== requestId) return
      setCandles(normalizeRows(recentRows, inv, isFutures))

      if (totalStart >= initialStart) return

      try {
        const olderRows = await fetchChartRange(sym, inv, isFutures, totalStart, initialStart, end)
        if (historyRequestIdRef.current !== requestId) return
        setCandles(normalizeRows([...olderRows, ...recentRows], inv, isFutures))
      } catch (e) {
        console.warn('Failed to fetch older chart history:', e)
      }
    } catch (e: any) {
      if (historyRequestIdRef.current !== requestId) return
      setError(e.message)
      setCandles([])
    }
  }, [fetchChartRange, normalizeRows])

  // Called when the chart is panned close to its oldest bar: pull one more page
  // of daily bars from the backend and replace the series with the longer one.
  const loadMoreDailyHistory = useCallback(async () => {
    const sym = activeSymbol
    if (!sym || chartInterval !== '1d' || !isActiveFutures) return

    const pageKey = getDailyPageKey(sym, isActiveFutures)
    if (loadingMoreRef.current || dailyExhausted.has(pageKey)) return

    const currentLimit = dailyLimitCache.get(pageKey) ?? DAILY_CHART_LIMIT
    const nextLimit = Math.min(currentLimit + DAILY_CHART_PAGE_BARS, MAX_DAILY_CHART_BARS)
    if (nextLimit <= currentLimit) {
      dailyExhausted.add(pageKey)
      return
    }

    loadingMoreRef.current = true
    const requestId = historyRequestIdRef.current
    try {
      const end = new Date()
      const start = new Date(end.getTime() - getHistoryLookbackHours(chartInterval) * 3600 * 1000)
      const queryEnd = new Date(end.getTime() + 24 * 3600 * 1000)
      const rows = await fetchChartRange(sym, chartInterval, isActiveFutures, start, queryEnd, end, nextLimit)
      // Bail out if the user switched symbol/interval while the page was in flight.
      if (historyRequestIdRef.current !== requestId) return

      dailyLimitCache.set(pageKey, nextLimit)
      // Fewer rows than asked for means the backend has nothing older left.
      if (rows.length < nextLimit) dailyExhausted.add(pageKey)

      const normalizedRows = normalizeRows(rows, chartInterval, isActiveFutures)
      if (normalizedRows.length === 0) return
      setCandles(normalizedRows)
    } catch (e) {
      console.warn('Failed to load older daily history:', e)
    } finally {
      loadingMoreRef.current = false
    }
  }, [activeSymbol, chartInterval, isActiveFutures, fetchChartRange, normalizeRows])

  const refreshRecentHistory = useCallback(async (sym: string, inv: string, isFutures: boolean) => {
    const seconds = intervalSeconds(inv)
    if (seconds < 60 || inv === '1d' || inv === '1w') return

    const requestId = ++recentRefreshRequestIdRef.current
    const end = new Date()
    const lookbackMs = Math.max(RECENT_HISTORY_REFRESH_BARS * seconds * 1000, 2 * 3600_000)
    const start = new Date(end.getTime() - lookbackMs)

    try {
      const rows = await fetchChartRange(sym, inv, isFutures, start, end, end)
      if (recentRefreshRequestIdRef.current !== requestId) return
      const normalizedRows = normalizeRows(rows, inv, isFutures)
      if (normalizedRows.length === 0) return

      setCandles(prev => dedupeAndSortCandles([...prev, ...normalizedRows]))
    } catch (e) {
      console.warn('Failed to refresh recent chart history:', e)
    }
  }, [fetchChartRange, normalizeRows])

  useEffect(() => {
    if (activeSymbol) {
      fetchHistory(activeSymbol, chartInterval, isActiveFutures)
    }
  }, [activeSymbol, fetchHistory, chartInterval, isActiveFutures, activeRollState?.active?.con_id])

  useEffect(() => {
    lastLiveBucketRef.current = null
    recentRefreshRequestIdRef.current += 1
    for (const timer of recentRefreshTimersRef.current) {
      clearTimeout(timer)
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
      const timer = setTimeout(() => {
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
      .then(contract => { if (!cancelled) setActiveContract(contract) })
      .catch(err => console.error('Failed to fetch active futures contract:', err))
    return () => { cancelled = true }
  }, [activeSymbol, isActiveFutures, activeRollState?.active?.con_id])

  const handleIntervalChange = useCallback((newInterval: string) => {
    setChartInterval(newInterval)
  }, [])

  const { colors } = useTheme()
  const contract = activeRollState?.active || activeContract

  return (
    <View style={[styles.container, { backgroundColor: colors.background }]}>
      {error && (
        <View style={[styles.errorBar, { backgroundColor: colors.dangerBg }]}>
          <Text style={styles.errorText}>{error}</Text>
        </View>
      )}

      {activeSymbol ? (
        <View style={styles.chartContainer}>
          {isActiveFutures && contract && (
            <View style={styles.contractBar}>
              <Text style={[styles.contractMain, { color: colors.textPrimary }]}>
                {activeSymbol} {contract.local_symbol || contract.contract_month || contract.con_id}
              </Text>
              <Text style={[styles.contractMeta, { color: colors.textSecondary }]}>
                conId {contract.con_id}{contract.contract_month ? `  ${contract.contract_month}` : ''}
              </Text>
            </View>
          )}
          <View style={styles.chartWrap}>
            <CandleChartRN
              symbol={activeSymbol!}
              data={candles}
              liveTick={chartLiveTick}
              interval={chartInterval}
              onIntervalChange={handleIntervalChange}
              onLoadMoreHistory={loadMoreDailyHistory}
            />
          </View>
        </View>
      ) : (
        <View style={styles.empty}>
          <Text style={{ color: colors.textMuted, fontSize: 13 }}>
            {'\u8BF7\u4ECE\u4E0A\u65B9\u4E0B\u62C9\u5217\u8868\u9009\u62E9\u4E00\u4E2A\u6807\u7684\u5F00\u59CB\u76D1\u63A7'}
          </Text>
        </View>
      )}
    </View>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  errorBar: { padding: 12, marginHorizontal: 8, marginTop: 8, borderRadius: 8 },
  errorText: { color: '#d32f2f', fontSize: 13 },
  chartContainer: { flex: 1 },
  contractBar: { paddingHorizontal: 10, paddingTop: 8, paddingBottom: 2 },
  contractMain: { fontSize: 12, fontWeight: '700', fontFamily: 'monospace' },
  contractMeta: { fontSize: 11, marginTop: 2 },
  chartWrap: { flex: 1 },
  empty: { flex: 1, alignItems: 'center', justifyContent: 'center' },
})
