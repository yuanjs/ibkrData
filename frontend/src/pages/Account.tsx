import { useState, useEffect, useRef } from 'react'
import { api } from '../api/client'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'
import { useMarketStore } from '../store/marketStore'
import { getProductConfig } from '../config/productConfig'

export function Account() {
  const activeGateway = useAccountStore(s => s.activeGateway)
  const setActiveGateway = useAccountStore(s => s.setActiveGateway)
  const hasPaper = useAccountStore(s => s.hasPaper)
  const summary = useAccountStore(s => activeGateway === 'live' ? s.live.summary : s.paper.summary)
  const positions = useAccountStore(s => activeGateway === 'live' ? s.live.positions : s.paper.positions)
  const gatewayMap = useAccountStore(s => s.gatewayMap)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)
  const [closeMsg, setCloseMsg] = useState<string | null>(null)

  // 加载 gateway map — 仅在 WebSocket 尚未推送时通过 HTTP 获取
  useEffect(() => {
    if (Object.keys(gatewayMap).length === 0) {
      api.get('/gateway/map').then(setGatewayMap).catch(() => {})
    }
  }, [gatewayMap, setGatewayMap])

  const fmt = (v: number | undefined) => v != null ? v.toLocaleString('en-US', { style: 'currency', currency: 'USD' }) : '-'
  const pnlColor = (v: number | undefined) => v == null ? '' : v >= 0 ? '#26a641' : '#d32f2f'

  // ===== 实时 PnL：用 tick 价格推算持仓市值变化 =====
  const quotes = useMarketStore(s => s.quotes)

  interface PnlRef {
    refPnl: number
    refMarketValue: number
    refPrice: number
  }
  const pnlRefs = useRef<Record<string, PnlRef>>({})

  // positions 或 quotes 变化时更新参考点（仅在 positions 变化时重置）
  const prevPositionsRef = useRef('')
  const positionsKey = JSON.stringify((positions as Array<Record<string, unknown>>).map(p => [p.symbol, p.market_value, p.unrealized_pnl]))
  useEffect(() => {
    if (positionsKey === prevPositionsRef.current) return
    prevPositionsRef.current = positionsKey
    const refs: Record<string, PnlRef> = {}
    for (const pos of (positions as Array<Record<string, unknown>>)) {
      const sym = pos.symbol as string
      const mv = pos.market_value as number | undefined
      const up = pos.unrealized_pnl as number | undefined
      const last = (quotes as Record<string, any>)?.[sym]?.last
      if (mv != null && up != null && last != null && last > 0) {
        refs[sym] = { refPnl: up, refMarketValue: mv, refPrice: last }
      }
    }
    if (Object.keys(refs).length) pnlRefs.current = refs
  }, [positionsKey, quotes])

  /** 计算实时未实现盈亏，返回 {pnl, isRealtime} */
  function realtimePnl(pos: Record<string, unknown>): { pnl: number | undefined; isRealtime: boolean } {
    const sym = pos.symbol as string
    const ref = pnlRefs.current[sym]
    if (!ref) {
      // 无参考点：用 tick 价格 + multiplier 自行估算
      const last = (quotes as Record<string, any>)?.[sym]?.last
      const qty = pos.quantity as number | undefined
      const avg = pos.avg_cost as number | undefined
      if (last != null && last > 0 && qty != null && qty !== 0 && avg != null && avg > 0) {
        const mult = getProductConfig(sym).multiplier ?? 1
        return { pnl: (last - avg) * qty * mult, isRealtime: true }
      }
      return { pnl: pos.unrealized_pnl as number | undefined, isRealtime: false }
    }
    const currentPrice = (quotes as Record<string, any>)?.[sym]?.last
    if (!currentPrice || !ref.refPrice || ref.refPrice <= 0 || !ref.refMarketValue) {
      return { pnl: pos.unrealized_pnl as number | undefined, isRealtime: false }
    }
    const ratio = currentPrice / ref.refPrice
    const estMv = ref.refMarketValue * ratio
    const estPnl = ref.refPnl + (estMv - ref.refMarketValue)
    return { pnl: estPnl, isRealtime: true }
  }

  function realtimeMarketValue(pos: Record<string, unknown>): { mv: number | undefined; isRealtime: boolean } {
    const sym = pos.symbol as string
    const ref = pnlRefs.current[sym]
    if (!ref) {
      // 无参考点：用 tick 价格 + multiplier 自行估算
      const last = (quotes as Record<string, any>)?.[sym]?.last
      const qty = pos.quantity as number | undefined
      if (last != null && last > 0 && qty != null && qty !== 0) {
        const mult = getProductConfig(sym).multiplier ?? 1
        return { mv: last * qty * mult, isRealtime: true }
      }
      return { mv: pos.market_value as number | undefined, isRealtime: false }
    }
    const currentPrice = (quotes as Record<string, any>)?.[sym]?.last
    if (!currentPrice || !ref.refPrice || ref.refPrice <= 0) {
      return { mv: pos.market_value as number | undefined, isRealtime: false }
    }
    return { mv: ref.refMarketValue * (currentPrice / ref.refPrice), isRealtime: true }
  }

  const RealtimeBadge = () => (
    <span style={{ fontSize: '0.6rem', color: 'var(--text-secondary)', marginLeft: 3, verticalAlign: 'super' }}>⚡</span>
  )

  // Watch for close order result via WebSocket
  useEffect(() => {
    if (!closePending) return
    const lastOrder = orders[0] as Record<string, unknown> | undefined
    if (lastOrder?.close_id === closePending.closeId) {
      if (lastOrder?.status === 'Filled') {
        setCloseMsg(`${closePending.symbol} 平仓成功 🎉`)
      } else if (lastOrder?.status === 'Rejected') {
        setCloseMsg(`${closePending.symbol} 平仓失败`)
      }
      const timer = setTimeout(() => { setClosePending(null); setCloseMsg(null) }, 4000)
      return () => clearTimeout(timer)
    }
  }, [orders, closePending])

  const handleClose = async (symbol: string) => {
    const pos = (positions as Array<Record<string, unknown>>).find(p => p.symbol === symbol)
    if (!pos) return
    const sideLabel = (pos.quantity as number) > 0 ? '卖出' : '买入'
    const qty = Math.abs(pos.quantity as number)
    if (!window.confirm(`确定以市价平仓 ${symbol}？\n方向: ${sideLabel}\n数量: ${qty}`)) return

    try {
      setCloseMsg(null)
      const res = await api.post<{ close_id: string }>('/positions/close', { symbol })
      setClosePending({ closeId: res.close_id, symbol })
      setCloseMsg(`平仓指令已发送: ${symbol}`)
    } catch (e: any) {
      setCloseMsg(`平仓失败: ${e.message}`)
    }
  }

  return (
    <div className="p-4 space-y-6">
      {/* Gateway 切换标签 — 仅在有 paper 账户时显示 */}
      {hasPaper && (
        <div className="flex gap-2 mb-2">
          <button onClick={() => setActiveGateway('live')}
            className={`px-4 py-1.5 text-sm rounded ${
              activeGateway === 'live'
                ? 'bg-blue-600 text-white'
                : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'
            }`}>
            实盘
          </button>
          <button onClick={() => setActiveGateway('paper')}
            className={`px-4 py-1.5 text-sm rounded ${
              activeGateway === 'paper'
                ? 'bg-blue-600 text-white'
                : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'
            }`}>
            模拟
          </button>
        </div>
      )}

      {closeMsg && (
        <div className="px-4 py-2 rounded text-sm"
          style={{ backgroundColor: closeMsg.includes('成功') ? '#1b5e20' : closeMsg.includes('失败') ? '#b71c1c' : 'var(--bg-surface)', color: '#fff' }}>
          {closeMsg}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: '净值', key: 'net_liquidation' },
          { label: '现金余额', key: 'total_cash' },
          { label: '可用资金', key: 'available_funds' },
          { label: '今日盈亏', key: 'daily_pnl' },
        ].map(({ label, key }) => (
          <div key={key} className="rounded-lg p-4" style={{ backgroundColor: 'var(--bg-surface)' }}>
            <div className="text-xs mb-1" style={{ color: 'var(--text-secondary)' }}>{label}</div>
            <div className="text-lg font-mono font-bold"
              style={{ color: key.includes('pnl') ? pnlColor(summary[key] as number) : 'var(--text-primary)' }}>
              {fmt(summary[key] as number)}
            </div>
          </div>
        ))}
      </div>

      <div className="overflow-x-auto">
        <h2 className="text-sm mb-2" style={{ color: 'var(--text-secondary)' }}>当前持仓</h2>
        <table className="w-full text-sm min-w-[700px] md:min-w-0">
          <thead>
            <tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-right py-2 px-3">数量</th>
              <th className="text-right py-2 px-3">均价</th>
              <th className="text-right py-2 px-3">市值</th>
              <th className="text-right py-2 px-3">未实现盈亏</th>
              <th className="text-center py-2 px-3">操作</th>
            </tr>
          </thead>
          <tbody>
            {(positions as Array<Record<string, unknown>>).map((p, i) => {
              const sym = p.symbol as string
              const isPending = closePending?.symbol === sym
              return (
                <tr key={i} className="border-b" style={{
                  borderColor: 'var(--border-light)',
                  opacity: isPending ? 0.6 : 1,
                }}>
                  <td className="py-2 px-3 font-mono font-bold" style={{ color: 'var(--text-primary)' }}>{sym}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{p.quantity as number}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{fmt(p.avg_cost as number)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>
                    {fmt(realtimeMarketValue(p).mv)}
                    {realtimeMarketValue(p).isRealtime && <RealtimeBadge />}
                  </td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: pnlColor(realtimePnl(p).pnl) }}>
                    {fmt(realtimePnl(p).pnl)}
                    {realtimePnl(p).isRealtime && <RealtimeBadge />}
                  </td>
                  <td className="py-2 px-3 text-center">
                    <button
                      onClick={() => handleClose(sym)}
                      disabled={!!isPending}
                      className="px-3 py-1 text-xs rounded font-medium"
                      style={{
                        backgroundColor: isPending ? 'var(--bg-raised)' : '#d32f2f',
                        color: isPending ? 'var(--text-secondary)' : '#fff',
                        border: 'none',
                        cursor: isPending ? 'not-allowed' : 'pointer',
                      }}
                    >
                      {isPending ? '平仓中...' : '平仓'}
                    </button>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
