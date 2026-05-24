import { useState, useCallback, useEffect } from 'react'
import { View, Text, StyleSheet } from 'react-native'
import { CandleChartRN } from '../src/components/CandleChartRN'
import { api } from '../src/api/client'
import { useMarketStore } from '../src/stores/marketStore'
import { useTheme } from '../src/theme'

export default function Monitor() {
  const activeSymbol = useMarketStore(s => s.activeSymbol)
  const setActiveSymbol = useMarketStore(s => s.setActiveSymbol)
  const quote = useMarketStore(s => activeSymbol ? s.quotes[activeSymbol] : null)
  const lastTick = useMarketStore(s => (s.lastTick?.symbol === activeSymbol) ? s.lastTick : null)

  const chartLiveTick = lastTick || (quote?.last ? {
    symbol: activeSymbol!,
    price: quote.last,
    time: quote.time,
    size: 0,
  } : null)

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
      let hours = 24
      if (inv.endsWith('s') || inv === '1m') hours = 6
      else if (inv === '1d') hours = 24 * 365
      else if (inv === '1w') hours = 24 * 365 * 2
      else if (inv.endsWith('m')) hours = 24 * 7
      else hours = 24 * 14

      const start = new Date(end.getTime() - hours * 3600 * 1000)
      const queryEnd = inv === '1d' ? new Date(end.getTime() + 24 * 3600 * 1000) : end

      const res = await api.get<{ time: string; open: number; high: number; low: number; close: number }[]>(
        `/history/${sym}?start=${start.toISOString()}&end=${queryEnd.toISOString()}&interval=${inv}`
      )
      setCandles(res.map((d: any) => {
        let t = Math.floor(new Date(d.time).getTime() / 1000)
        if (inv === '1d') {
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

  const handleIntervalChange = useCallback((newInterval: string) => {
    setChartInterval(newInterval)
    if (activeSymbol) {
      fetchHistory(activeSymbol, newInterval)
    }
  }, [activeSymbol, fetchHistory])

  const { colors } = useTheme()

  return (
    <View style={[styles.container, { backgroundColor: colors.background }]}>
      {error && (
        <View style={[styles.errorBar, { backgroundColor: colors.dangerBg }]}>
          <Text style={styles.errorText}>{error}</Text>
        </View>
      )}

      {activeSymbol ? (
        <View style={styles.chartContainer}>
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
  chartWrap: { flex: 1 },
  empty: { flex: 1, alignItems: 'center', justifyContent: 'center' },
})
