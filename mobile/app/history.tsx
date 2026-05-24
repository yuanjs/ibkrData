import { useState } from 'react'
import { View, Text, TextInput, TouchableOpacity, StyleSheet, ScrollView, Alert, Platform } from 'react-native'
import DateTimePicker, { type DateTimePickerEvent } from '@react-native-community/datetimepicker'
import { File, Paths, Directory } from 'expo-file-system'
import * as Sharing from 'expo-sharing'
import { api } from '../src/api/client'
import { CandleChartRN } from '../src/components/CandleChartRN'
import { useTheme } from '../src/theme'

export default function History() {
  const [symbol, setSymbol] = useState('')
  const [startDate, setStartDate] = useState(new Date(Date.now() - 7 * 86400_000))
  const [endDate, setEndDate] = useState(new Date())
  const [interval, setIntervalVal] = useState('1min')
  const [candles, setCandles] = useState<any[]>([])
  const [showStartPicker, setShowStartPicker] = useState(false)
  const [showEndPicker, setShowEndPicker] = useState(false)
  const [loading, setLoading] = useState(false)
  const { colors } = useTheme()

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
      const data = await api.get<any[]>(
        `/history/${symbol}?start=${startDate.toISOString()}&end=${endDate.toISOString()}&interval=${interval}`
      )
      setCandles(Array.isArray(data) ? data.map((d: any) => {
        let t = Math.floor(new Date(d.time).getTime() / 1000)
        if (interval === '1d') {
          const dt = new Date(t * 1000)
          t = Math.floor(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), 12) / 1000)
        }
        return { ...d, time: t }
      }) : [])
    } catch (e: any) {
      Alert.alert('查询失败', e.message)
    } finally {
      setLoading(false)
    }
  }

  const exportCSV = async () => {
    try {
      const base = process.env.EXPO_PUBLIC_API_URL || 'http://192.168.1.100:8002'
      const token = process.env.EXPO_PUBLIC_API_TOKEN || 'dev-token'
      const url = `${base}/api/history/${symbol}/export?start=${startDate.toISOString()}&end=${endDate.toISOString()}&interval=${interval}`
      const file = await File.downloadFileAsync(url, new Directory(Paths.document), {
        headers: { Authorization: `Bearer ${token}` },
      })
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
        <View style={styles.chartWrap}>
          <CandleChartRN
            symbol={symbol}
            data={candles}
            liveTick={null}
            interval={interval}
            onIntervalChange={setIntervalVal}
          />
        </View>
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
  chartWrap: { height: 400, marginTop: 12 },
})
