import { useCallback, useEffect, useMemo, useState } from 'react'
import { View, Text, TouchableOpacity, StyleSheet, ScrollView, Alert, Platform } from 'react-native'
import DateTimePicker, { type DateTimePickerEvent } from '@react-native-community/datetimepicker'
import { File, Paths, Directory } from 'expo-file-system'
import * as Sharing from 'expo-sharing'
import { api } from '../src/api/client'
import { useTheme, type ThemeColors } from '../src/theme'
import { getSymbolDecimalPlaces } from '../src/config/productConfig'
import { useOrderStore } from '../src/stores/orderStore'
import { useMarketStore, type Quote } from '../src/stores/marketStore'

type TabKey = 'orders' | 'trades' | 'pnl'
type RangePreset = '24h' | '3d' | '7d' | '1m' | 'custom'

const RANGE_PRESETS: { key: RangePreset; label: string }[] = [
  { key: '24h', label: '24小时' },
  { key: '3d', label: '3天' },
  { key: '7d', label: '7天' },
  { key: '1m', label: '1个月' },
  { key: 'custom', label: '自定义' },
]

const presetRange = (preset: Exclude<RangePreset, 'custom'>) => {
  const end = new Date()
  const start = new Date(end)
  if (preset === '1m') {
    const day = start.getDate()
    start.setDate(1)
    start.setMonth(start.getMonth() - 1)
    const lastDay = new Date(start.getFullYear(), start.getMonth() + 1, 0).getDate()
    start.setDate(Math.min(day, lastDay))
  }
  else start.setHours(start.getHours() - (preset === '24h' ? 24 : preset === '3d' ? 72 : 168))
  return { start, end }
}

const rangeParams = (start: Date | null, end: Date | null) => {
  const params = new URLSearchParams()
  if (start) params.set('start', start.toISOString())
  if (end) params.set('end', end.toISOString())
  const text = params.toString()
  return text ? `?${text}` : ''
}

const formatCompactDateTime = (value: unknown) => {
  if (!value) return '-'
  const date = new Date(value as string)
  const pad = (part: number) => String(part).padStart(2, '0')
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())}\n${pad(date.getHours())}:${pad(date.getMinutes())}`
}

const quotePrice = (quote?: Quote) => {
  if (quote?.last != null && quote.last > 0) return quote.last
  if (quote?.bid != null && quote.bid > 0 && quote?.ask != null && quote.ask > 0) {
    return (quote.bid + quote.ask) / 2
  }
  return quote?.bid != null && quote.bid > 0 ? quote.bid : quote?.ask != null && quote.ask > 0 ? quote.ask : null
}

const convertPnlToAud = (value: number, currency: string, audUsdRate: number | null, usdJpyRate: number | null) => {
  switch (currency.toUpperCase()) {
    case 'AUD': return value
    case 'USD': return audUsdRate ? value / audUsdRate : null
    case 'JPY': return audUsdRate && usdJpyRate ? value / (audUsdRate * usdJpyRate) : null
    default: return null
  }
}

export default function Orders() {
  const [initialRange] = useState(() => presetRange('24h'))
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [pnl, setPnl] = useState<unknown[]>([])
  const [tab, setTab] = useState<TabKey>('orders')
  const [rangePreset, setRangePreset] = useState<RangePreset>('24h')
  const [startDate, setStartDate] = useState<Date | null>(initialRange.start)
  const [endDate, setEndDate] = useState<Date | null>(initialRange.end)
  const [appliedStart, setAppliedStart] = useState<Date | null>(initialRange.start)
  const [appliedEnd, setAppliedEnd] = useState<Date | null>(initialRange.end)
  const [showStartPicker, setShowStartPicker] = useState(false)
  const [showEndPicker, setShowEndPicker] = useState(false)
  const [showStartTimePicker, setShowStartTimePicker] = useState(false)
  const [showEndTimePicker, setShowEndTimePicker] = useState(false)
  const [loading, setLoading] = useState(false)
  const { colors } = useTheme()
  const wsOrderCount = useOrderStore(s => s.orders.length)
  const audUsdQuote = useMarketStore(s => s.quotes['AUD.USD'] ?? s.quotes.AUDUSD)
  const usdJpyQuote = useMarketStore(s => s.quotes['USD.JPY'] ?? s.quotes.USDJPY)
  const audUsdRate = quotePrice(audUsdQuote)
  const usdJpyRate = quotePrice(usdJpyQuote)

  const fetchData = useCallback(() => {
    const controller = new AbortController()
    const endpoint = tab === 'orders' ? '/orders' : tab === 'trades' ? '/trades' : '/pnl'
    const range = rangePreset === 'custom' ? { start: appliedStart, end: appliedEnd } : presetRange(rangePreset)
    setLoading(true)
    api.get(`${endpoint}${rangeParams(range.start, range.end)}`, { signal: controller.signal })
      .then(data => {
        const rows = Array.isArray(data) ? data : []
        if (tab === 'orders') setOrders(rows)
        else if (tab === 'trades') setTrades(rows)
        else setPnl(rows)
      })
      .catch(error => {
        if (error instanceof Error && error.name !== 'AbortError') Alert.alert('加载失败', error.message)
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [appliedEnd, appliedStart, rangePreset, tab])

  // 只加载当前页签；条件变化或 WebSocket 更新时取消旧请求并刷新。
  useEffect(() => {
    let cancelled = false
    let abortRequest: (() => void) | undefined
    queueMicrotask(() => {
      if (!cancelled) abortRequest = fetchData()
    })
    return () => {
      cancelled = true
      abortRequest?.()
    }
  }, [fetchData, wsOrderCount])

  const pnlSummary = useMemo(() => {
    const groups = new Map<string, { symbol: string; currency: string; realized_pnl: number; trade_count: number }>()
    for (const row of pnl as Record<string, unknown>[]) {
      const symbol = String(row.symbol ?? '-')
      const currency = String(row.currency ?? '')
      const key = `${symbol}:${currency}`
      const group = groups.get(key) ?? { symbol, currency, realized_pnl: 0, trade_count: 0 }
      group.realized_pnl += Number(row.realized_pnl ?? 0)
      group.trade_count += 1
      groups.set(key, group)
    }
    return [...groups.values()]
      .map(group => ({
        ...group,
        realized_pnl_aud: convertPnlToAud(group.realized_pnl, group.currency, audUsdRate, usdJpyRate),
      }))
      .sort((a, b) => a.symbol.localeCompare(b.symbol))
  }, [audUsdRate, pnl, usdJpyRate])

  const onStartChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowStartPicker(Platform.OS === 'ios')
    if (date) {
      if (Platform.OS === 'ios') setStartDate(date)
      else {
        const next = new Date(startDate ?? date)
        next.setFullYear(date.getFullYear(), date.getMonth(), date.getDate())
        setStartDate(next)
        setShowStartTimePicker(true)
      }
    }
  }

  const onEndChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowEndPicker(Platform.OS === 'ios')
    if (date) {
      if (Platform.OS === 'ios') setEndDate(date)
      else {
        const next = new Date(endDate ?? date)
        next.setFullYear(date.getFullYear(), date.getMonth(), date.getDate())
        setEndDate(next)
        setShowEndTimePicker(true)
      }
    }
  }

  const onStartTimeChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowStartTimePicker(false)
    if (date) {
      const next = new Date(startDate ?? date)
      next.setHours(date.getHours(), date.getMinutes(), 0, 0)
      setStartDate(next)
    }
  }

  const onEndTimeChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowEndTimePicker(false)
    if (date) {
      const next = new Date(endDate ?? date)
      next.setHours(date.getHours(), date.getMinutes(), 59, 999)
      setEndDate(next)
    }
  }

  const applyRange = () => {
    setAppliedStart(startDate)
    setAppliedEnd(endDate)
  }

  const selectPreset = (preset: RangePreset) => {
    setRangePreset(preset)
    if (preset !== 'custom') {
      const range = presetRange(preset)
      setStartDate(range.start)
      setEndDate(range.end)
      setAppliedStart(range.start)
      setAppliedEnd(range.end)
    }
  }

  const exportCSV = async () => {
    try {
      const base = process.env.EXPO_PUBLIC_API_URL || 'http://192.168.1.100:8002'
      const token = process.env.EXPO_PUBLIC_API_TOKEN || 'dev-token'
      const range = rangePreset === 'custom' ? { start: appliedStart, end: appliedEnd } : presetRange(rangePreset)
      const url = `${base}/api/trades/export${rangeParams(range.start, range.end)}`
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

  const tabs: { key: TabKey; label: string }[] = [
    { key: 'orders', label: '订单' },
    { key: 'trades', label: '成交' },
    { key: 'pnl', label: '盈亏报告' },
  ]

  return (
    <ScrollView style={[styles.container, { backgroundColor: colors.background }]}>
      <View style={styles.tabRow}>
        {tabs.map(t => (
          <TouchableOpacity
            key={t.key}
            onPress={() => setTab(t.key)}
            style={[
              styles.tabBtn,
              {
                backgroundColor: tab === t.key ? '#2563eb' : colors.raised,
              },
            ]}
          >
            <Text style={{ color: tab === t.key ? '#fff' : colors.textSecondary, fontSize: 13 }}>
              {t.label}
            </Text>
          </TouchableOpacity>
        ))}
        {tab === 'trades' && (
          <TouchableOpacity onPress={exportCSV} style={[styles.exportBtn, { backgroundColor: colors.raised }]}>
            <Text style={{ color: colors.textSecondary, fontSize: 12 }}>导出CSV</Text>
          </TouchableOpacity>
        )}
      </View>

      <View style={styles.filterRow}>
        {RANGE_PRESETS.map(option => (
          <TouchableOpacity key={option.key} onPress={() => selectPreset(option.key)}
            style={[styles.rangeBtn, { backgroundColor: rangePreset === option.key ? '#2563eb' : colors.raised }]}>
            <Text style={{ color: rangePreset === option.key ? '#fff' : colors.textSecondary, fontSize: 12 }}>{option.label}</Text>
          </TouchableOpacity>
        ))}
        {rangePreset === 'custom' && <>
          <TouchableOpacity onPress={() => setShowStartPicker(true)} style={[styles.dateBtn, { backgroundColor: colors.raised, borderColor: colors.border }]}>
            <Text style={{ color: colors.textPrimary, fontSize: 12 }}>{startDate ? `开始 ${startDate.toLocaleString()}` : '选择开始时间'}</Text>
          </TouchableOpacity>
          {showStartPicker && <DateTimePicker value={startDate ?? new Date()} mode={Platform.OS === 'ios' ? 'datetime' : 'date'} onChange={onStartChange} />}
          {showStartTimePicker && <DateTimePicker value={startDate ?? new Date()} mode="time" onChange={onStartTimeChange} />}
          <TouchableOpacity onPress={() => setShowEndPicker(true)} style={[styles.dateBtn, { backgroundColor: colors.raised, borderColor: colors.border }]}>
            <Text style={{ color: colors.textPrimary, fontSize: 12 }}>{endDate ? `结束 ${endDate.toLocaleString()}` : '选择结束时间'}</Text>
          </TouchableOpacity>
          {showEndPicker && <DateTimePicker value={endDate ?? new Date()} mode={Platform.OS === 'ios' ? 'datetime' : 'date'} onChange={onEndChange} />}
          {showEndTimePicker && <DateTimePicker value={endDate ?? new Date()} mode="time" onChange={onEndTimeChange} />}
          <TouchableOpacity onPress={applyRange}
            disabled={!startDate || !endDate || startDate > endDate || (startDate.getTime() === appliedStart?.getTime() && endDate.getTime() === appliedEnd?.getTime())}
            style={[styles.queryBtn, { opacity: !startDate || !endDate || startDate > endDate || (startDate.getTime() === appliedStart?.getTime() && endDate.getTime() === appliedEnd?.getTime()) ? 0.5 : 1 }]}>
            <Text style={{ color: '#fff', fontSize: 12 }}>查询</Text>
          </TouchableOpacity>
          {startDate && endDate && startDate > endDate && <Text style={styles.rangeError}>开始时间不能晚于结束时间</Text>}
        </>}
      </View>

      {loading && <Text style={[styles.loading, { color: colors.textSecondary }]}>加载中...</Text>}

      {tab === 'orders' && renderTable(orders, [
        { label: '标的', flex: 1.2 },
        { label: '方向', flex: 0.75 },
        { label: '数量', flex: 0.8, align: 'right' },
        { label: '价格', flex: 1.15, align: 'right' },
        { label: '状态', flex: 1.1 },
      ], colors, o => [
        { text: o.symbol as string, mono: true, bold: true },
        { text: o.action as string, mono: false, color: o.action === 'BUY' ? '#26a641' : '#d32f2f' },
        { text: String(o.quantity ?? ''), mono: true, align: 'right' },
        { text: o.limit_price != null ? String(o.limit_price) : '-', mono: true, align: 'right' },
        { text: o.status as string, mono: false, align: 'left' },
      ])}

      {tab === 'trades' && renderTable(trades, [
        { label: '时间', flex: 1.2 },
        { label: '标的', flex: 0.85 },
        { label: '方向', flex: 0.65 },
        { label: '数量', flex: 0.8, align: 'right' },
        { label: '价格', flex: 1.5, align: 'right' },
        { label: '费用', flex: 0.8, align: 'right' },
      ], colors, t => [
        { text: formatCompactDateTime(t.time), mono: true, size: 11, lines: 2 },
        { text: t.symbol as string, mono: true, bold: true },
        { text: t.side as string, mono: false, color: t.side === 'BOT' ? '#26a641' : '#d32f2f' },
        { text: String(t.quantity ?? ''), mono: true, align: 'right' },
        { text: t.price != null ? (t.price as number).toFixed(getSymbolDecimalPlaces(t.symbol as string)) : '', mono: true, align: 'right' },
        { text: t.commission != null ? String(t.commission) : '', mono: false, align: 'right' },
      ])}

      {tab === 'pnl' && renderTable(pnlSummary, [
        { label: '标的', flex: 0.9 },
        { label: '币种', flex: 0.6 },
        { label: '原币盈亏', flex: 1.25, align: 'right' },
        { label: 'AUD盈亏', flex: 1.3, align: 'right' },
        { label: '次数', flex: 0.55, align: 'right' },
      ], colors, p => [
        { text: p.symbol as string, mono: true, bold: true },
        { text: String(p.currency || '-'), mono: true },
        { text: p.realized_pnl != null ? (p.realized_pnl as number).toFixed(2) : '', mono: true, align: 'right', color: (p.realized_pnl as number) >= 0 ? '#26a641' : '#d32f2f' },
        { text: p.realized_pnl_aud != null ? Number(p.realized_pnl_aud).toFixed(2) : '-', mono: true, align: 'right', color: p.realized_pnl_aud == null ? colors.textMuted : Number(p.realized_pnl_aud) >= 0 ? '#26a641' : '#d32f2f' },
        { text: String(p.trade_count ?? ''), mono: false, align: 'right' },
      ])}
    </ScrollView>
  )
}

interface CellDef {
  text: string
  mono?: boolean
  bold?: boolean
  color?: string
  align?: 'left' | 'right'
  size?: number
  lines?: number
}

interface ColumnDef {
  label: string
  flex: number
  align?: 'left' | 'right'
}

function renderTable(data: unknown[], columns: ColumnDef[], colors: ThemeColors, cellMapper: (item: Record<string, unknown>) => CellDef[]) {
  return (
    <View style={[tableStyles.wrapper, { backgroundColor: colors.elevated, borderColor: colors.border }]}>
      <View style={[tableStyles.headerRow, { backgroundColor: colors.surface, borderBottomColor: colors.border }]}>
        {columns.map(column => (
          <Text key={column.label} numberOfLines={2} style={[
            tableStyles.headerText,
            { flex: column.flex, color: colors.textSecondary, textAlign: column.align ?? 'left' },
          ]}>
            {column.label}
          </Text>
        ))}
      </View>
      {(data as Record<string, unknown>[]).map((item, i) => (
        <View key={i} style={[
          tableStyles.dataRow,
          { backgroundColor: i % 2 === 0 ? colors.elevated : colors.surface, borderBottomColor: colors.borderLight },
        ]}>
          {cellMapper(item).map((cell, j) => (
            <Text
              key={j}
              numberOfLines={cell.lines ?? 1}
              style={[
                tableStyles.cell,
                { flex: columns[j]?.flex ?? 1, textAlign: cell.align ?? columns[j]?.align ?? 'left' },
                cell.mono ? { fontFamily: 'monospace' } : undefined,
                cell.bold ? { fontWeight: '700' } : undefined,
                cell.color ? { color: cell.color } : { color: colors.textPrimary },
                cell.size != null ? { fontSize: cell.size } : undefined,
              ]}
            >
              {cell.text}
            </Text>
          ))}
        </View>
      ))}
      {data.length === 0 && (
        <View style={tableStyles.emptyRow}>
          <Text style={[tableStyles.emptyText, { color: colors.textMuted }]}>暂无数据</Text>
        </View>
      )}
    </View>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 12 },
  tabRow: { flexDirection: 'row', gap: 8, marginBottom: 12, flexWrap: 'wrap' },
  filterRow: { flexDirection: 'row', gap: 8, marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' },
  rangeBtn: { paddingHorizontal: 11, paddingVertical: 7, borderRadius: 6 },
  tabBtn: { paddingHorizontal: 14, paddingVertical: 7, borderRadius: 6 },
  exportBtn: { paddingHorizontal: 12, paddingVertical: 7, borderRadius: 6, marginLeft: 'auto' },
  dateBtn: { paddingHorizontal: 10, paddingVertical: 7, borderRadius: 6, borderWidth: 1 },
  queryBtn: { paddingHorizontal: 13, paddingVertical: 7, borderRadius: 6, backgroundColor: '#2563eb' },
  rangeError: { color: '#d32f2f', fontSize: 12 },
  loading: { fontSize: 12, marginBottom: 8 },
})

const tableStyles = StyleSheet.create({
  wrapper: { borderWidth: 1, borderRadius: 10, overflow: 'hidden', marginBottom: 20 },
  headerRow: { flexDirection: 'row', borderBottomWidth: 1, minHeight: 46, alignItems: 'center' },
  headerText: { flexBasis: 0, fontSize: 13, lineHeight: 17, fontWeight: '700', letterSpacing: 0.1, paddingHorizontal: 4 },
  dataRow: { flexDirection: 'row', borderBottomWidth: StyleSheet.hairlineWidth, minHeight: 52, alignItems: 'center' },
  cell: { flexBasis: 0, fontSize: 13, lineHeight: 17, paddingHorizontal: 4 },
  emptyRow: { minHeight: 96, alignItems: 'center', justifyContent: 'center' },
  emptyText: { fontSize: 13 },
})
