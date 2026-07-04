import { useState, useCallback, useEffect, useRef } from 'react'
import { View, Text, StyleSheet } from 'react-native'
import { CandleChartRN } from '../src/components/CandleChartRN'
import { api, futuresApi, type FuturesActiveContract, type SymbolSubscription } from '../src/api/client'
import { useMarketStore } from '../src/stores/marketStore'
import { useTheme } from '../src/theme'
import { aggregateCandles, getFuturesDailyAsOf, normalizeCandles, type CandleLike } from '../src/utils/chartData'

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
      return api.get<{ time: string; open: number; high: number; low: number; close: number }[]>(
        `/history/${sym}?start=${start.toISOString()}&end=${rangeEnd.toISOString()}&interval=${inv}`
      )
    }

    const normalizeRows = (rows: CandleLike[]) => {
      const chartRows = isFutures && inv !== '1d' ? aggregateCandles(rows, inv) : rows
      return dedupeAndSortCandles(normalizeCandles(chartRows, inv))
    }

    try {
      setError(null)
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
