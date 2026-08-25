import { useCallback, useEffect, useMemo, useState } from 'react'
import { View, Text, TouchableOpacity, StyleSheet, ScrollView, Alert, Platform } from 'react-native'
import DateTimePicker, { type DateTimePickerEvent } from '@react-native-community/datetimepicker'
import { File, Paths, Directory } from 'expo-file-system'
import * as Sharing from 'expo-sharing'
import { api } from '../src/api/client'
import { useTheme } from '../src/theme'
import { getSymbolDecimalPlaces } from '../src/config/productConfig'
import { useOrderStore } from '../src/stores/orderStore'

type TabKey = 'orders' | 'trades' | 'pnl'

const rangeParams = (start: Date | null, end: Date | null) => {
  const params = new URLSearchParams()
  if (start) params.set('start', start.toISOString())
  if (end) params.set('end', end.toISOString())
  const text = params.toString()
  return text ? `?${text}` : ''
}

export default function Orders() {
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [pnl, setPnl] = useState<unknown[]>([])
  const [tab, setTab] = useState<TabKey>('orders')
  const [startDate, setStartDate] = useState<Date | null>(null)
  const [endDate, setEndDate] = useState<Date | null>(null)
  const [appliedStart, setAppliedStart] = useState<Date | null>(null)
  const [appliedEnd, setAppliedEnd] = useState<Date | null>(null)
  const [showStartPicker, setShowStartPicker] = useState(false)
  const [showEndPicker, setShowEndPicker] = useState(false)
  const [loading, setLoading] = useState(false)
  const { colors } = useTheme()
  const wsOrderCount = useOrderStore(s => s.orders.length)

  const fetchData = useCallback(() => {
    const controller = new AbortController()
    const endpoint = tab === 'orders' ? '/orders' : tab === 'trades' ? '/trades' : '/pnl'
    setLoading(true)
    api.get(`${endpoint}${rangeParams(appliedStart, appliedEnd)}`, { signal: controller.signal })
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
  }, [appliedEnd, appliedStart, tab])

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
    const groups = new Map<string, { symbol: string; realized_pnl: number; trade_count: number }>()
    for (const row of pnl as Record<string, unknown>[]) {
      const symbol = String(row.symbol ?? '-')
      const group = groups.get(symbol) ?? { symbol, realized_pnl: 0, trade_count: 0 }
      group.realized_pnl += Number(row.realized_pnl ?? 0)
      group.trade_count += 1
      groups.set(symbol, group)
    }
    return [...groups.values()].sort((a, b) => a.symbol.localeCompare(b.symbol))
  }, [pnl])

  const onStartChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowStartPicker(Platform.OS === 'ios')
    if (date) {
      const startOfDay = new Date(date)
      startOfDay.setHours(0, 0, 0, 0)
      setStartDate(startOfDay)
    }
  }

  const onEndChange = (_: DateTimePickerEvent, date?: Date) => {
    setShowEndPicker(Platform.OS === 'ios')
    if (date) {
      const endOfDay = new Date(date)
      endOfDay.setHours(23, 59, 59, 999)
      setEndDate(endOfDay)
    }
  }

  const applyRange = () => {
    setAppliedStart(startDate)
    setAppliedEnd(endDate)
  }

  const clearRange = () => {
    setStartDate(null)
    setEndDate(null)
    setAppliedStart(null)
    setAppliedEnd(null)
  }

  const exportCSV = async () => {
    try {
      const base = process.env.EXPO_PUBLIC_API_URL || 'http://192.168.1.100:8002'
      const token = process.env.EXPO_PUBLIC_API_TOKEN || 'dev-token'
      const url = `${base}/api/trades/export${rangeParams(appliedStart, appliedEnd)}`
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
        <TouchableOpacity onPress={() => setShowStartPicker(true)} style={[styles.dateBtn, { backgroundColor: colors.raised, borderColor: colors.border }]}>
          <Text style={{ color: colors.textPrimary, fontSize: 12 }}>{startDate ? `开始 ${startDate.toLocaleDateString()}` : '开始 不限'}</Text>
        </TouchableOpacity>
        {showStartPicker && <DateTimePicker value={startDate ?? new Date()} mode="date" onChange={onStartChange} />}
        <TouchableOpacity onPress={() => setShowEndPicker(true)} style={[styles.dateBtn, { backgroundColor: colors.raised, borderColor: colors.border }]}>
          <Text style={{ color: colors.textPrimary, fontSize: 12 }}>{endDate ? `结束 ${endDate.toLocaleDateString()}` : '结束 不限'}</Text>
        </TouchableOpacity>
        {showEndPicker && <DateTimePicker value={endDate ?? new Date()} mode="date" onChange={onEndChange} />}
        {(startDate || endDate) && (
          <TouchableOpacity onPress={clearRange} style={[styles.smallBtn, { backgroundColor: colors.raised }]}>
            <Text style={{ color: colors.textSecondary, fontSize: 12 }}>清除</Text>
          </TouchableOpacity>
        )}
        <TouchableOpacity onPress={applyRange} disabled={startDate?.getTime() === appliedStart?.getTime() && endDate?.getTime() === appliedEnd?.getTime()} style={[styles.queryBtn, { opacity: startDate?.getTime() === appliedStart?.getTime() && endDate?.getTime() === appliedEnd?.getTime() ? 0.5 : 1 }]}>
          <Text style={{ color: '#fff', fontSize: 12 }}>查询</Text>
        </TouchableOpacity>
      </View>

      {loading && <Text style={[styles.loading, { color: colors.textSecondary }]}>加载中...</Text>}

      {tab === 'orders' && renderTable(orders, ['标的', '方向', '数量', '价格', '状态'], colors, o => [
        { text: o.symbol as string, mono: true, bold: false },
        { text: o.action as string, mono: false, color: o.action === 'BUY' ? '#26a641' : '#d32f2f' },
        { text: String(o.quantity ?? ''), mono: true, align: 'right' },
        { text: o.limit_price != null ? String(o.limit_price) : '-', mono: true, align: 'right' },
        { text: o.status as string, mono: false, align: 'left' },
      ])}

      {tab === 'trades' && renderTable(trades, ['时间', '标的', '方向', '数量', '价格', '手续费'], colors, t => [
        { text: t.time ? new Date(t.time as string).toLocaleString() : '', mono: false, size: 11 },
        { text: t.symbol as string, mono: true, bold: false },
        { text: t.side as string, mono: false, color: t.side === 'BOT' ? '#26a641' : '#d32f2f' },
        { text: String(t.quantity ?? ''), mono: true, align: 'right' },
        { text: t.price != null ? (t.price as number).toFixed(getSymbolDecimalPlaces(t.symbol as string)) : '', mono: true, align: 'right' },
        { text: t.commission != null ? String(t.commission) : '', mono: false, align: 'right' },
      ])}

      {tab === 'pnl' && renderTable(pnlSummary, ['标的', '已实现盈亏', '平仓次数'], colors, p => [
        { text: p.symbol as string, mono: true, bold: false },
        { text: p.realized_pnl != null ? (p.realized_pnl as number).toFixed(2) : '', mono: true, align: 'right', color: (p.realized_pnl as number) >= 0 ? '#26a641' : '#d32f2f' },
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
}

function renderTable(data: unknown[], headers: string[], colors: any, cellMapper: (item: Record<string, unknown>) => CellDef[]) {
  return (
    <View style={tableStyles.wrapper}>
      <View style={[tableStyles.headerRow, { borderBottomColor: colors.border }]}>
        {headers.map(h => (
          <Text key={h} style={[tableStyles.headerText, { color: colors.textSecondary }, headers.length > 4 && { flex: 1 }]}>
            {h}
          </Text>
        ))}
      </View>
      <ScrollView horizontal showsHorizontalScrollIndicator={true}>
        <View>
          {(data as Record<string, unknown>[]).map((item, i) => (
            <View key={i} style={[tableStyles.dataRow, { borderBottomColor: colors.borderLight }]}>
              {cellMapper(item).map((cell, j) => (
                <Text
                  key={j}
                  style={[
                    tableStyles.cell,
                    cell.mono ? { fontFamily: 'monospace' } : undefined,
                    cell.bold ? { fontWeight: '700' } : undefined,
                    cell.color ? { color: cell.color } : { color: colors.textPrimary },
                    cell.align === 'right' ? { textAlign: 'right' } : undefined,
                    cell.size != null ? { fontSize: cell.size } : undefined,
                    headers.length > 4 ? { flex: 1 } : undefined,
                  ]}
                >
                  {cell.text}
                </Text>
              ))}
            </View>
          ))}
        </View>
      </ScrollView>
    </View>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 12 },
  tabRow: { flexDirection: 'row', gap: 8, marginBottom: 12, flexWrap: 'wrap' },
  filterRow: { flexDirection: 'row', gap: 8, marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' },
  tabBtn: { paddingHorizontal: 14, paddingVertical: 7, borderRadius: 6 },
  exportBtn: { paddingHorizontal: 12, paddingVertical: 7, borderRadius: 6, marginLeft: 'auto' },
  dateBtn: { paddingHorizontal: 10, paddingVertical: 7, borderRadius: 6, borderWidth: 1 },
  smallBtn: { paddingHorizontal: 10, paddingVertical: 7, borderRadius: 6 },
  queryBtn: { paddingHorizontal: 13, paddingVertical: 7, borderRadius: 6, backgroundColor: '#2563eb' },
  loading: { fontSize: 12, marginBottom: 8 },
})

const tableStyles = StyleSheet.create({
  wrapper: {},
  headerRow: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 8, paddingHorizontal: 6 },
  headerText: { fontSize: 12, flex: 1 },
  dataRow: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 10, paddingHorizontal: 6 },
  cell: { fontSize: 13, flex: 1 },
})
