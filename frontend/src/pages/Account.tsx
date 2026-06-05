import { useState, useEffect } from 'react'
import { api } from '../api/client'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'

export function Account() {
  const summary = useAccountStore(s => s.summary) as Record<string, number>
  const positions = useAccountStore(s => s.positions) as Array<Record<string, unknown>>
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)
  const [closeMsg, setCloseMsg] = useState<string | null>(null)

  const fmt = (v: number | undefined) => v != null ? v.toLocaleString('en-US', { style: 'currency', currency: 'USD' }) : '-'
  const pnlColor = (v: number | undefined) => v == null ? '' : v >= 0 ? '#26a641' : '#d32f2f'

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
      // Clear pending state after a brief delay so user sees result
      const timer = setTimeout(() => { setClosePending(null); setCloseMsg(null) }, 4000)
      return () => clearTimeout(timer)
    }
  }, [orders, closePending])

  const handleClose = async (symbol: string) => {
    const pos = positions.find(p => p.symbol === symbol) as Record<string, unknown> | undefined
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
              style={{ color: key.includes('pnl') ? pnlColor(summary[key]) : 'var(--text-primary)' }}>
              {fmt(summary[key])}
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
            {positions.map((p, i) => {
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
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{fmt(p.market_value as number)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: pnlColor(p.unrealized_pnl as number) }}>
                    {fmt(p.unrealized_pnl as number)}
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
