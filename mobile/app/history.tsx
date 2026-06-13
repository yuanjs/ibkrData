import { useEffect, useState } from 'react'
import { View, Text, TextInput, TouchableOpacity, StyleSheet, ScrollView, Alert, Platform } from 'react-native'
import DateTimePicker, { type DateTimePickerEvent } from '@react-native-community/datetimepicker'
import { File, Paths } from 'expo-file-system'
import * as Sharing from 'expo-sharing'
import { api, futuresApi, type FuturesActiveContract, type SymbolSubscription } from '../src/api/client'
import { CandleChartRN } from '../src/components/CandleChartRN'
import { useTheme } from '../src/theme'
import { aggregateCandles, candlesToCsv, getFuturesDailyAsOf, normalizeCandles, type CandleLike } from '../src/utils/chartData'

export default function History() {
  const [symbol, setSymbol] = useState('')
  const [subscriptions, setSubscriptions] = useState<Record<string, SymbolSubscription>>({})
  const [activeContract, setActiveContract] = useState<FuturesActiveContract | null>(null)
  const [startDate, setStartDate] = useState(new Date(Date.now() - 7 * 86400_000))
  const [endDate, setEndDate] = useState(new Date())
  const [interval, setIntervalVal] = useState('1min')
  const [candles, setCandles] = useState<any[]>([])
  const [showStartPicker, setShowStartPicker] = useState(false)
  const [showEndPicker, setShowEndPicker] = useState(false)
  const [loading, setLoading] = useState(false)
  const { colors } = useTheme()
  const isFutures = subscriptions[symbol]?.sec_type === 'FUT'

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
    setStartDate(s)
    setEndDate(e)
  }

  const search = async () => {
    if (!symbol) return
    setLoading(true)
    try {
      setActiveContract(null)
      let data: CandleLike[]
      if (isFutures) {
        const asOf = getFuturesDailyAsOf(symbol, endDate)
        const contract = await futuresApi.activeContract(symbol, asOf)
        setActiveContract(contract)
        if (interval === '1d') {
          data = await futuresApi.daily(symbol, startDate.toISOString(), asOf, 'back_adjusted', true)
        } else {
          data = await futuresApi.minute(symbol, startDate.toISOString(), endDate.toISOString(), 'active_raw', asOf)
          data = aggregateCandles(data, interval)
        }
      } else {
        data = await api.get<any[]>(
          `/history/${symbol}?start=${startDate.toISOString()}&end=${endDate.toISOString()}&interval=${interval}`
        )
      }
      setCandles(Array.isArray(data) ? normalizeCandles(data, interval) : [])
    } catch (e: any) {
      Alert.alert('查询失败', e.message)
    } finally {
      setLoading(false)
    }
  }

  const exportCSV = async () => {
    if (!candles.length) {
      return
    }
    try {
      const filename = `${symbol || 'history'}_${interval}_${new Date().toISOString().replace(/[:.]/g, '-')}.csv`
      const file = new File(Paths.document, filename)
      file.create({ overwrite: true })
      file.write(candlesToCsv(candles, symbol, interval))
      if (await Sharing.isAvailableAsync()) {
        await Sharing.shareAsync(file.uri)
      } else {
        Alert.alert('导出完成')
      }
    } catch (e: any) {
      Alert.alert('导出失败', e.message)
    }
  }

  const onStartChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowStartPicker(Platform.OS === 'ios')
    if (date) setStartDate(date)
  }
  const onEndChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowEndPicker(Platform.OS === 'ios')
    if (date) setEndDate(date)
  }

  const intervals = ['1s', '1min', '5min', '1h']

  return (
    <ScrollView style={[styles.container, { backgroundColor: colors.background }]}>
      <View style={styles.formRow}>
        <TextInput
          style={[styles.input, { backgroundColor: colors.surface, color: colors.textPrimary, borderColor: colors.borderDarker }]}
          placeholder="标的"
          placeholderTextColor={colors.textMuted}
          value={symbol}
          onChangeText={t => setSymbol(t.toUpperCase())}
          autoCapitalize="characters"
        />

        <TouchableOpacity
          onPress={() => setShowStartPicker(true)}
          style={[styles.dateBtn, { backgroundColor: colors.surface, borderColor: colors.borderDarker }]}
        >
          <Text style={{ color: colors.textPrimary, fontSize: 12 }}>
            {startDate.toLocaleDateString()}
          </Text>
        </TouchableOpacity>
        {showStartPicker && (
          <DateTimePicker value={startDate} mode="date" onChange={onStartChange} />
        )}

        <TouchableOpacity
          onPress={() => setShowEndPicker(true)}
          style={[styles.dateBtn, { backgroundColor: colors.surface, borderColor: colors.borderDarker }]}
        >
          <Text style={{ color: colors.textPrimary, fontSize: 12 }}>
            {endDate.toLocaleDateString()}
          </Text>
        </TouchableOpacity>
        {showEndPicker && (
          <DateTimePicker value={endDate} mode="date" onChange={onEndChange} />
        )}

        {intervals.map(inv => (
          <TouchableOpacity
            key={inv}
            onPress={() => setIntervalVal(inv)}
            style={[styles.smallBtn, { backgroundColor: interval === inv ? '#2563eb' : colors.raised }]}
          >
            <Text style={{ color: interval === inv ? '#fff' : colors.textSecondary, fontSize: 11, fontFamily: 'monospace' }}>
              {inv}
            </Text>
          </TouchableOpacity>
        ))}
      </View>

      <View style={styles.quickRow}>
        {[['今日', 1], ['近7天', 7], ['近30天', 30]].map(([label, days]) => (
          <TouchableOpacity
            key={label as string}
            onPress={() => quick(days as number)}
            style={[styles.quickBtn, { backgroundColor: colors.raised }]}
          >
            <Text style={{ color: colors.textSecondary, fontSize: 11 }}>{label as string}</Text>
          </TouchableOpacity>
        ))}
      </View>

      <View style={styles.actionRow}>
        <TouchableOpacity
          onPress={search}
          disabled={loading || !symbol}
          style={[styles.primaryBtn, { opacity: (!symbol || loading) ? 0.5 : 1 }]}
        >
          <Text style={{ color: '#fff', fontSize: 13 }}>{loading ? '查询中...' : '查询'}</Text>
        </TouchableOpacity>
        {candles.length > 0 && (
          <TouchableOpacity onPress={exportCSV} style={[styles.secondaryBtn, { backgroundColor: colors.raised }]}>
            <Text style={{ color: colors.textSecondary, fontSize: 12 }}>导出CSV</Text>
          </TouchableOpacity>
        )}
      </View>

      {candles.length > 0 && (
        <>
          {isFutures && activeContract && (
            <View style={styles.contractBar}>
              <Text style={[styles.contractMain, { color: colors.textPrimary }]}>
                {symbol} {activeContract.local_symbol || activeContract.contract_month || activeContract.con_id}
              </Text>
              <Text style={[styles.contractMeta, { color: colors.textSecondary }]}>
                conId {activeContract.con_id}{activeContract.contract_month ? `  ${activeContract.contract_month}` : ''}
              </Text>
            </View>
          )}
        <View style={styles.chartWrap}>
          <CandleChartRN
            symbol={symbol}
            data={candles}
            liveTick={null}
            interval={interval}
            onIntervalChange={setIntervalVal}
          />
        </View>
        </>
      )}
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 12 },
  formRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, alignItems: 'center' },
  input: {
    borderWidth: 1,
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 6,
    fontSize: 13,
    width: 80,
    fontFamily: 'monospace',
  },
  dateBtn: {
    borderWidth: 1,
    borderRadius: 6,
    paddingHorizontal: 8,
    paddingVertical: 6,
  },
  smallBtn: {
    paddingHorizontal: 8,
    paddingVertical: 5,
    borderRadius: 4,
  },
  quickRow: { flexDirection: 'row', gap: 4, marginTop: 8 },
  quickBtn: { paddingHorizontal: 8, paddingVertical: 5, borderRadius: 4 },
  actionRow: { flexDirection: 'row', gap: 8, marginTop: 10 },
  primaryBtn: {
    backgroundColor: '#2563eb',
    paddingHorizontal: 16,
    paddingVertical: 7,
    borderRadius: 6,
  },
  secondaryBtn: { paddingHorizontal: 12, paddingVertical: 7, borderRadius: 6 },
  contractBar: { marginTop: 10 },
  contractMain: { fontSize: 12, fontWeight: '700', fontFamily: 'monospace' },
  contractMeta: { fontSize: 11, marginTop: 2 },
  chartWrap: { height: 400, marginTop: 12 },
})
