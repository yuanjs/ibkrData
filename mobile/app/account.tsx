import { View, Text, StyleSheet, ScrollView } from 'react-native'
import { useAccountStore } from '../src/stores/accountStore'
import { useTheme } from '../src/theme'

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
  const { colors } = useTheme()

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
        </View>

        {positions.map((p, i) => (
          <View key={i} style={[styles.tableRow, { borderBottomColor: colors.borderLight }]}>
            <Text style={[styles.td, styles.colSym, { color: colors.textPrimary, fontFamily: 'monospace', fontWeight: '700' }]}>
              {p.symbol as string}
            </Text>
            <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
              {p.quantity as number}
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
          </View>
        ))}
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
})
