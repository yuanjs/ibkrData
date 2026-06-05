import { useState, useEffect, useRef } from 'react'
import { View, Text, StyleSheet, ScrollView, TouchableOpacity, Alert } from 'react-native'
import { useAccountStore } from '../src/stores/accountStore'
import { useMarketStore } from '../src/stores/marketStore'
import { useTheme } from '../src/theme'
import { api } from '../src/api/client'
import { useOrderStore } from '../src/stores/orderStore'
import { getProductConfig, getSymbolDecimalPlaces } from '../src/config/productConfig'

function fmt(v: number | undefined) {
  if (v == null) return '-'
  return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function fmtPrice(v: number | undefined, sym?: string) {
  if (v == null) return '-'
  const d = sym ? getSymbolDecimalPlaces(sym) : 2
  return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d })
}

function pnlColor(v: number | undefined) {
  if (v == null) return undefined
  return v >= 0 ? '#26a641' : '#d32f2f'
}

interface PnlRef {
  refPnl: number
  refMarketValue: number
  refPrice: number
}

export default function Account() {
  const activeGateway = useAccountStore(s => s.activeGateway)
  const setActiveGateway = useAccountStore(s => s.setActiveGateway)
  const hasPaper = useAccountStore(s => s.hasPaper)
  const gatewayMap = useAccountStore(s => s.gatewayMap)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const summary = useAccountStore(s => activeGateway === 'live' ? s.live.summary : s.paper.summary)
  const positions = useAccountStore(s => activeGateway === 'live' ? s.live.positions : s.paper.positions)
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const { colors } = useTheme()
  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)

  // Gateway map load
  useEffect(() => {
    if (Object.keys(gatewayMap).length === 0) {
      api.get<Record<string, string[]>>('/gateway/map').then(setGatewayMap).catch(() => {})
    }
  }, [gatewayMap, setGatewayMap])

  // REST fallback on page load（store 在 gatewayMap 未到时自动放 live 显示）
  useEffect(() => {
    const stored = useAccountStore.getState()
    if (Object.keys(stored.live.summary).length > 0) return
    Promise.all([
      api.get<Record<string, unknown>[]>('/account'),
      api.get<Record<string, unknown>[]>('/positions'),
    ]).then(([accounts, positions]) => {
      if (Array.isArray(accounts) && accounts.length) {
        useAccountStore.getState().setAccount({
          accounts,
          positions: Array.isArray(positions) ? positions : [],
        })
      }
    }).catch(() => {})
  }, [])

  // Real-time PnL
  const quotes = useMarketStore(s => s.quotes)
  const quotesRef = useRef(quotes)
  quotesRef.current = quotes
  const pnlRefs = useRef<Record<string, PnlRef>>({})

  const prevPosRef = useRef('')
  const posKey = JSON.stringify((positions as Array<Record<string, unknown>>).map(p => [p.symbol, p.market_value, p.unrealized_pnl]))
  useEffect(() => {
    if (posKey === prevPosRef.current) return
    prevPosRef.current = posKey
    const refs: Record<string, PnlRef> = {}
    for (const pos of (positions as Array<Record<string, unknown>>)) {
      const sym = pos.symbol as string
      const mv = pos.market_value as number | undefined
      const up = pos.unrealized_pnl as number | undefined
      const last = (quotesRef.current as Record<string, any>)?.[sym]?.last
      if (mv != null && up != null && last != null && last > 0) {
        refs[sym] = { refPnl: up, refMarketValue: mv, refPrice: last }
      }
    }
    if (Object.keys(refs).length) pnlRefs.current = refs
  }, [posKey])

  function getMult(pos: Record<string, unknown>): number {
    const fromPos = pos.multiplier as number | undefined
    if (fromPos != null && fromPos > 0) return fromPos
    return getProductConfig(pos.symbol as string).multiplier ?? 1
  }

  function entryPrice(pos: Record<string, unknown>): string {
    const avg = pos.avg_cost as number | undefined
    if (avg == null) return '-'
    return fmtPrice(avg / getMult(pos), pos.symbol as string)
  }

  function currentQuote(pos: Record<string, unknown>): string {
    const q = (quotesRef.current as Record<string, any>)?.[pos.symbol as string]
    if (q?.bid != null && q.bid > 0 && q?.ask != null && q.ask > 0) {
      return `${fmtPrice(q.bid, pos.symbol as string)} / ${fmtPrice(q.ask, pos.symbol as string)}`
    }
    if (q?.last != null && q.last > 0) return fmtPrice(q.last, pos.symbol as string)
    return '-'
  }

  function calcPnl(pos: Record<string, unknown>): { pnl: number | undefined; isRealtime: boolean } {
    const sym = pos.symbol as string
    const ref = pnlRefs.current[sym]
    if (ref) {
      const p = (quotesRef.current as Record<string, any>)?.[sym]?.last
      if (p && p > 0 && ref.refPrice > 0 && ref.refMarketValue) {
        const ratio = p / ref.refPrice
        return { pnl: ref.refPnl + (ref.refMarketValue * ratio - ref.refMarketValue), isRealtime: true }
      }
      return { pnl: pos.unrealized_pnl as number | undefined, isRealtime: false }
    }
    // Fallback with IBKR multiplier
    const last = (quotesRef.current as Record<string, any>)?.[sym]?.last
    const qty = pos.quantity as number | undefined
    const avg = pos.avg_cost as number | undefined
    if (last != null && last > 0 && qty != null && qty !== 0 && avg != null && avg > 0) {
      const mult = getMult(pos)
      return { pnl: (last * mult - avg) * qty, isRealtime: true }
    }
    return { pnl: pos.unrealized_pnl as number | undefined, isRealtime: false }
  }

  // Close order watch
  useEffect(() => {
    if (!closePending) return
    const lastOrder = orders[0] as Record<string, unknown> | undefined
    if (lastOrder?.close_id === closePending.closeId) {
      const s = lastOrder?.status as string
      if (s === 'Filled') {
        Alert.alert('平仓成功', `${closePending.symbol} 已平仓`)
        setClosePending(null)
      } else if (s === 'Rejected' || s === 'Cancelled' || s === 'Inactive') {
        Alert.alert('平仓失败', `${closePending.symbol} ${s === 'Cancelled' ? '已取消' : '被拒绝'}`)
        setClosePending(null)
      }
    }
    // 超时自动清除（30 秒）
    const timer = setTimeout(() => {
      Alert.alert('平仓超时', `${closePending.symbol} 未收到成交回报，请手动检查订单状态`)
      setClosePending(null)
    }, 30000)
    return () => clearTimeout(timer)
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
              const res = await api.post<{ close_id: string }>('/positions/close', {
                symbol,
                gateway: activeGateway,
              })
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
      {/* Gateway tabs */}
      {hasPaper && (
        <View style={{ flexDirection: 'row', gap: 8, marginBottom: 10 }}>
          {(['live', 'paper'] as const).map(g => (
            <TouchableOpacity key={g} onPress={() => setActiveGateway(g)}
              style={[styles.tabBtn, {
                backgroundColor: activeGateway === g ? '#2563eb' : colors.surface,
              }]}>
              <Text style={{ color: activeGateway === g ? '#fff' : colors.textSecondary, fontSize: 14, fontWeight: '600' }}>
                {g === 'live' ? '实盘' : '模拟'}
              </Text>
            </TouchableOpacity>
          ))}
        </View>
      )}

      <View style={styles.cardGrid}>
        {cards.map(({ label, key }) => (
          <View key={key} style={[styles.card, { backgroundColor: colors.surface }]}>
            <Text style={[styles.cardLabel, { color: colors.textSecondary }]}>{label}</Text>
            <Text
              style={[
                styles.cardValue,
                { color: key.includes('pnl') ? pnlColor(summary[key] as number) ?? colors.textPrimary : colors.textPrimary },
              ]}
            >
              {fmt(summary[key] as number)}
            </Text>
          </View>
        ))}
      </View>

      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textSecondary }]}>当前持仓</Text>

        <View style={[styles.tableHeader, { borderBottomColor: colors.border }]}>
          <Text style={[styles.th, styles.colSym, { color: colors.textSecondary }]}>标的</Text>
          <Text style={[styles.th, styles.colDir, { color: colors.textSecondary }]}>方向</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>数量</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>开仓价</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>当前报价</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>盈亏</Text>
          <Text style={[styles.th, styles.colAction, { color: colors.textSecondary }]}>操作</Text>
        </View>

        {(positions as Array<Record<string, unknown>>).map((p) => {
          const symbol = p.symbol as string
          const quantity = p.quantity as number
          const isPending = closePending?.symbol === symbol
          const pnl = calcPnl(p)
          return (
            <View
              key={symbol}
              style={[
                styles.tableRow,
                { borderBottomColor: colors.borderLight, opacity: isPending ? 0.6 : 1 },
              ]}
            >
              <Text style={[styles.td, styles.colSym, { color: colors.textPrimary, fontFamily: 'monospace', fontWeight: '700' }]}>
                {symbol}
              </Text>
              <Text style={[styles.td, styles.colDir, {
                color: quantity > 0 ? '#26a641' : quantity < 0 ? '#d32f2f' : colors.textPrimary,
                fontFamily: 'monospace', fontSize: 12, fontWeight: '600',
              }]}>
                {quantity > 0 ? '多' : quantity < 0 ? '空' : '-'}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {Math.abs(quantity)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {entryPrice(p)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {currentQuote(p)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: pnlColor(pnl.pnl) ?? colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(pnl.pnl)}
                {pnl.isRealtime ? '⚡' : ''}
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
  tabBtn: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 6,
  },
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
  colDir: { flex: 0.5, textAlign: 'center' },
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
