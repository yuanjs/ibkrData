import { useState, useEffect } from 'react'
import { View, Text, StyleSheet, ScrollView, TouchableOpacity, Alert } from 'react-native'
import { useAccountStore } from '../src/stores/accountStore'
import { useTheme } from '../src/theme'
import { api } from '../src/api/client'
import { useOrderStore } from '../src/stores/orderStore'

function fmt(v: number | undefined) {
  if (v == null) return '-'
  return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function pnlColor(v: number | undefined) {
  if (v == null) return undefined
  return v >= 0 ? '#26a641' : '#d32f2f'
}

export default function Account() {
  const summary = useAccountStore(s => s.summary) as Record<string, number>
  const positions = useAccountStore(s => s.positions) as Array<Record<string, unknown>>
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const { colors } = useTheme()

  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)

  // Watch for the close order to be filled
  useEffect(() => {
    if (!closePending) return
    const lastOrder = orders[0] as Record<string, unknown> | undefined
    if (lastOrder?.close_id === closePending.closeId && lastOrder?.status === 'Filled') {
      Alert.alert('平仓成功', `${closePending.symbol} 已平仓`)
      setClosePending(null)
    }
  }, [orders, closePending])

  const handleClose = (symbol: string, quantity: number) => {
    const direction = quantity >= 0 ? '做多' : '做空'
    const absQty = Math.abs(quantity)
    Alert.alert(
      '确认平仓',
      `标的: ${symbol}\n方向: ${direction}\n数量: ${absQty}`,
      [
        { text: '取消', style: 'cancel' },
        {
          text: '确认',
          style: 'destructive',
          onPress: async () => {
            try {
              const res = await api.post<{ close_id: string }>('/positions/close', { symbol })
              setClosePending({ closeId: res.close_id, symbol })
            } catch (e: unknown) {
              const msg = e instanceof Error ? e.message : String(e)
              Alert.alert('平仓失败', msg)
            }
          },
        },
      ],
    )
  }

  const cards = [
    { label: '净值', key: 'net_liquidation' },
    { label: '现金余额', key: 'total_cash' },
    { label: '可用资金', key: 'available_funds' },
    { label: '今日盈亏', key: 'daily_pnl' },
  ]

  return (
    <ScrollView style={[styles.container, { backgroundColor: colors.background }]}>
      <View style={styles.cardGrid}>
        {cards.map(({ label, key }) => (
          <View key={key} style={[styles.card, { backgroundColor: colors.surface }]}>
            <Text style={[styles.cardLabel, { color: colors.textSecondary }]}>{label}</Text>
            <Text
              style={[
                styles.cardValue,
                { color: key.includes('pnl') ? pnlColor(summary[key]) ?? colors.textPrimary : colors.textPrimary },
              ]}
            >
              {fmt(summary[key])}
            </Text>
          </View>
        ))}
      </View>

      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textSecondary }]}>当前持仓</Text>

        {/* Table header */}
        <View style={[styles.tableHeader, { borderBottomColor: colors.border }]}>
          <Text style={[styles.th, styles.colSym, { color: colors.textSecondary }]}>标的</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>数量</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>均价</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>市值</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>盈亏</Text>
          <Text style={[styles.th, styles.colAction, { color: colors.textSecondary }]}>操作</Text>
        </View>

        {positions.map((p, i) => {
          const symbol = p.symbol as string
          const quantity = p.quantity as number
          const isPending = closePending?.symbol === symbol
          return (
            <View
              key={i}
              style={[
                styles.tableRow,
                { borderBottomColor: colors.borderLight, opacity: isPending ? 0.6 : 1 },
              ]}
            >
              <Text style={[styles.td, styles.colSym, { color: colors.textPrimary, fontFamily: 'monospace', fontWeight: '700' }]}>
                {symbol}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {quantity}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(p.avg_cost as number)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(p.market_value as number)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: pnlColor(p.unrealized_pnl as number) ?? colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(p.unrealized_pnl as number)}
              </Text>
              <View style={styles.colAction}>
                <TouchableOpacity
                  onPress={() => handleClose(symbol, quantity)}
                  disabled={isPending}
                  style={[
                    styles.closeBtn,
                    { backgroundColor: isPending ? '#888' : '#d32f2f' },
                  ]}
                >
                  <Text style={styles.closeBtnText}>{isPending ? '平仓中' : '平仓'}</Text>
                </TouchableOpacity>
              </View>
            </View>
          )
        })}
      </View>
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 12 },
  cardGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  card: {
    width: '48%',
    borderRadius: 8,
    padding: 14,
  },
  cardLabel: { fontSize: 12, marginBottom: 4 },
  cardValue: { fontSize: 17, fontFamily: 'monospace', fontWeight: '700' },
  section: { marginTop: 20 },
  sectionTitle: { fontSize: 13, marginBottom: 8 },
  tableHeader: {
    flexDirection: 'row',
    borderBottomWidth: 1,
    paddingVertical: 8,
    paddingHorizontal: 8,
  },
  th: { fontSize: 12 },
  tableRow: {
    flexDirection: 'row',
    borderBottomWidth: 1,
    paddingVertical: 10,
    paddingHorizontal: 8,
  },
  td: { fontSize: 13 },
  colSym: { flex: 1.2 },
  colNum: { flex: 1, textAlign: 'right' },
  colAction: { flex: 0.6, alignItems: 'center', justifyContent: 'center' },
  closeBtn: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 4,
  },
  closeBtnText: {
    color: '#fff',
    fontSize: 12,
    fontWeight: '600',
  },
})
