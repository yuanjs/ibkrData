import { useEffect, useState, useCallback } from 'react'
import { api } from '../api/client'
import { getSymbolDecimalPlaces } from '../config/productConfig'
import { useAccountStore } from '../store/accountStore'

type GatewayFilter = '' | 'live' | 'paper'

export function Orders() {
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [tab, setTab] = useState<'orders' | 'trades' | 'pnl'>('orders')
  const [pnl, setPnl] = useState<unknown[]>([])
  const [gateway, setGateway] = useState<GatewayFilter>('')

  const gatewayMap = useAccountStore(s => s.gatewayMap)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const hasPaper = useAccountStore(s => s.hasPaper)

  // 页面刷新后通过 REST 加载 gateway map（不等 WebSocket）
  useEffect(() => {
    if (Object.keys(gatewayMap).length === 0) {
      api.get<Record<string, string[]>>('/gateway/map').then(setGatewayMap).catch(() => {})
    }
  }, [gatewayMap, setGatewayMap])

  const fetchData = useCallback(() => {
    const params = gateway ? `?gateway=${gateway}` : ''
    api.get(`/orders${params}`).then(d => { if (Array.isArray(d)) setOrders(d) })
    api.get(`/trades${params}`).then(d => { if (Array.isArray(d)) setTrades(d) })
    api.get(`/pnl${params}`).then(d => { if (Array.isArray(d)) setPnl(d) })
  }, [gateway])

  useEffect(() => { fetchData() }, [fetchData])

  const gwLabel = (account_id: string | undefined) => {
    if (!account_id) return ''
    return gatewayMap[account_id] ?? ''
  }

  const gwBadge = (account_id: string | undefined) => {
    const g = gwLabel(account_id)
    if (!g) return null
    return (
      <span style={{
        display: 'inline-block',
        fontSize: '0.65rem',
        padding: '1px 6px',
        borderRadius: 4,
        backgroundColor: g === 'live' ? '#1a5fb4' : '#5e5e5e',
        color: '#fff',
        marginLeft: 6,
        verticalAlign: 'middle',
      }}>
        {g === 'live' ? '实盘' : '模拟'}
      </span>
    )
  }

  const GatewayTabs = () => (
    <div className="flex gap-2 mb-3">
      {(['', 'live'] as GatewayFilter[]).map(g => (
        <button key={g} onClick={() => setGateway(g)}
          className={`px-3 py-1 text-xs rounded ${
            gateway === g
              ? 'bg-blue-600 text-white'
              : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'
          }`}>
          {g === '' ? '全部' : '实盘'}
        </button>
      ))}
      {hasPaper && (
        <button onClick={() => setGateway('paper')}
          className={`px-3 py-1 text-xs rounded ${
            gateway === 'paper'
              ? 'bg-blue-600 text-white'
              : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'
          }`}>
          模拟
        </button>
      )}
    </div>
  )

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
          <a href={`/api/trades/export${gateway ? `?gateway=${gateway}` : ''}`}
            className="ml-auto px-3 py-1.5 text-sm rounded hover:bg-[var(--bg-hover)]"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>
            导出CSV
          </a>
        )}
      </div>

      <GatewayTabs />

      {tab === 'orders' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[550px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th><th className="text-left py-2 px-3">方向</th>
              <th className="text-right py-2 px-3">数量</th><th className="text-right py-2 px-3">价格</th>
              <th className="text-left py-2 px-3">状态</th>
            </tr></thead>
            <tbody>{(orders as Record<string, unknown>[]).map((o, i) => (
              <tr key={i} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>
                  {o.symbol as string}
                  {gwBadge(o.account_id as string | undefined)}
                </td>
                <td className="py-2 px-3" style={{ color: o.action === 'BUY' ? '#26a641' : '#d32f2f' }}>{o.action as string}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{o.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{(o.limit_price as number)?.toFixed(getSymbolDecimalPlaces(o.symbol as string)) ?? '-'}</td>
                <td className="py-2 px-3" style={{ color: 'var(--text-secondary)' }}>{o.status as string}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {tab === 'trades' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[650px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">时间</th><th className="text-left py-2 px-3">标的</th>
              <th className="text-left py-2 px-3">方向</th><th className="text-right py-2 px-3">数量</th>
              <th className="text-right py-2 px-3">价格</th><th className="text-right py-2 px-3">手续费</th>
            </tr></thead>
            <tbody>{(trades as Record<string, unknown>[]).map((t, i) => (
              <tr key={i} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 text-xs" style={{ color: 'var(--text-secondary)' }}>{new Date(t.time as string).toLocaleString()}</td>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>
                  {t.symbol as string}
                  {gwBadge(t.account_id as string | undefined)}
                </td>
                <td className="py-2 px-3" style={{ color: t.side === 'BOT' ? '#26a641' : '#d32f2f' }}>{t.side as string}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{t.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{(t.price as number)?.toFixed(getSymbolDecimalPlaces(t.symbol as string))}</td>
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
                <td className="py-2 px-3 text-right font-mono" style={{ color: (p.realized_pnl as number) >= 0 ? '#26a641' : '#d32f2f' }}>
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
