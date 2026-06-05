import { useEffect, useState, useRef } from 'react'
import { View, Text, TouchableOpacity, StyleSheet, ScrollView, Alert } from 'react-native'
import { File, Paths, Directory } from 'expo-file-system'
import * as Sharing from 'expo-sharing'
import { api } from '../src/api/client'
import { useTheme } from '../src/theme'
import { getSymbolDecimalPlaces } from '../src/config/productConfig'
import { useOrderStore } from '../src/stores/orderStore'

type TabKey = 'orders' | 'trades' | 'pnl'

export default function Orders() {
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [pnl, setPnl] = useState<unknown[]>([])
  const [tab, setTab] = useState<TabKey>('orders')
  const { colors } = useTheme()
  const wsOrderCount = useOrderStore(s => s.orders.length)

  const fetchData = () => {
    api.get('/orders').then(d => { if (Array.isArray(d)) setOrders(d) })
    api.get('/trades').then(d => { if (Array.isArray(d)) setTrades(d) })
    api.get('/pnl').then(d => { if (Array.isArray(d)) setPnl(d) })
  }

  useEffect(() => { fetchData() }, [])

  // WebSocket 有新的订单推送时自动刷新
  const prevCount = useRef(wsOrderCount)
  useEffect(() => {
    if (wsOrderCount !== prevCount.current) {
      prevCount.current = wsOrderCount
      fetchData()
    }
  }, [wsOrderCount])

  const exportCSV = async () => {
    try {
      const base = process.env.EXPO_PUBLIC_API_URL || 'http://192.168.1.100:8002'
      const token = process.env.EXPO_PUBLIC_API_TOKEN || 'dev-token'
      const url = `${base}/api/trades/export`
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

      {tab === 'pnl' && renderTable(pnl, ['标的', '已实现盈亏', '交易次数'], colors, p => [
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
  tabBtn: { paddingHorizontal: 14, paddingVertical: 7, borderRadius: 6 },
  exportBtn: { paddingHorizontal: 12, paddingVertical: 7, borderRadius: 6, marginLeft: 'auto' },
})

const tableStyles = StyleSheet.create({
  wrapper: {},
  headerRow: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 8, paddingHorizontal: 6 },
  headerText: { fontSize: 12, flex: 1 },
  dataRow: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 10, paddingHorizontal: 6 },
  cell: { fontSize: 13, flex: 1 },
})
