import { useEffect, useState } from 'react'
import { api } from '../api/client'

export function Orders() {
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [tab, setTab] = useState<'orders' | 'trades' | 'pnl'>('orders')
  const [pnl, setPnl] = useState<unknown[]>([])

  useEffect(() => {
    api.get('/orders').then(d => { if (Array.isArray(d)) setOrders(d) })
    api.get('/trades').then(d => { if (Array.isArray(d)) setTrades(d) })
    api.get('/pnl').then(d => { if (Array.isArray(d)) setPnl(d) })
  }, [])

  return (
    <div className="p-4">
      <div className="flex gap-2 mb-4">
        {(['orders', 'trades', 'pnl'] as const).map(t => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-4 py-1.5 text-sm rounded ${tab === t ? 'bg-blue-600 text-white' : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'}`}>
            {t === 'orders' ? '订单' : t === 'trades' ? '成交' : '盈亏报告'}
          </button>
        ))}
        {tab === 'trades' && (
          <a href="/api/trades/export" className="ml-auto px-3 py-1.5 text-sm rounded hover:bg-[var(--bg-hover)]" style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>
            导出CSV
          </a>
        )}
      </div>

      {tab === 'orders' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[500px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th><th className="text-left py-2 px-3">方向</th>
              <th className="text-right py-2 px-3">数量</th><th className="text-right py-2 px-3">价格</th>
              <th className="text-left py-2 px-3">状态</th>
            </tr></thead>
            <tbody>{(orders as Record<string, unknown>[]).map((o, i) => (
              <tr key={i} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>{o.symbol as string}</td>
                <td className={`py-2 px-3 ${o.action === 'BUY' ? 'text-green-400' : 'text-red-400'}`}>{o.action as string}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{o.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{o.limit_price as number ?? '-'}</td>
                <td className="py-2 px-3" style={{ color: 'var(--text-secondary)' }}>{o.status as string}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {tab === 'trades' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[600px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">时间</th><th className="text-left py-2 px-3">标的</th>
              <th className="text-left py-2 px-3">方向</th><th className="text-right py-2 px-3">数量</th>
              <th className="text-right py-2 px-3">价格</th><th className="text-right py-2 px-3">手续费</th>
            </tr></thead>
            <tbody>{(trades as Record<string, unknown>[]).map((t, i) => (
              <tr key={i} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 text-xs" style={{ color: 'var(--text-secondary)' }}>{new Date(t.time as string).toLocaleString()}</td>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>{t.symbol as string}</td>
                <td className={`py-2 px-3 ${t.side === 'BOT' ? 'text-green-400' : 'text-red-400'}`}>{t.side as string}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{t.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{t.price as number}</td>
                <td className="py-2 px-3 text-right" style={{ color: 'var(--text-secondary)' }}>{t.commission as number}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {tab === 'pnl' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[400px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-right py-2 px-3">已实现盈亏</th>
              <th className="text-right py-2 px-3">交易次数</th>
            </tr></thead>
            <tbody>{(pnl as Record<string, unknown>[]).map((p, i) => (
              <tr key={i} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>{p.symbol as string}</td>
                <td className={`py-2 px-3 text-right font-mono ${(p.realized_pnl as number) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {(p.realized_pnl as number)?.toFixed(2)}
                </td>
                <td className="py-2 px-3 text-right" style={{ color: 'var(--text-secondary)' }}>{p.trade_count as number}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}
    </div>
  )
}
