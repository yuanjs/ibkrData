import { useEffect, useState, useCallback, useRef } from 'react'
import { api } from '../api/client'
import { getSymbolDecimalPlaces } from '../config/productConfig'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'

const formatNumber = (value: unknown, decimals?: number, fallback = '-') => {
  const num = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(num) ? num.toFixed(decimals) : fallback
}

const formatDateTime = (value: unknown) => value ? new Date(value as string).toLocaleString() : '-'

const toApiDate = (value: string) => value ? new Date(value).toISOString() : ''

const rangeParams = (gateway: string | null, start: string, end: string) => {
  const params = new URLSearchParams()
  if (gateway) params.set('gateway', gateway)
  if (start) params.set('start', toApiDate(start))
  if (end) params.set('end', toApiDate(end))
  const text = params.toString()
  return text ? `?${text}` : ''
}

type PnlGroup = {
  symbol: string
  realized_pnl: number
  trade_count: number
  rows: Record<string, unknown>[]
}

export function Orders() {
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [tab, setTab] = useState<'orders' | 'trades' | 'pnl'>('orders')
  const [pnl, setPnl] = useState<unknown[]>([])
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const connectedGateway = useAccountStore(s => s.connectedGateway)
  const accountIds = useAccountStore(s => s.accountIds)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const wsOrderCount = useOrderStore(s => s.orders.length)

  // 页面刷新后通过 REST 加载 gateway map（不等 WebSocket）
  useEffect(() => {
    api.get<Record<string, unknown>>('/gateway/map').then(setGatewayMap).catch(() => {})
  }, [setGatewayMap])

  const fetchData = useCallback(() => {
    const params = rangeParams(connectedGateway, start, end)
    setLoading(true)
    setError(null)
    Promise.all([
      api.get(`/orders${params}`),
      api.get(`/trades${params}`),
      api.get(`/pnl${params}`),
    ])
      .then(([ordersData, tradesData, pnlData]) => {
        setOrders(Array.isArray(ordersData) ? ordersData : [])
        setTrades(Array.isArray(tradesData) ? tradesData : [])
        setPnl(Array.isArray(pnlData) ? pnlData : [])
      })
      .catch(err => {
        setError(err instanceof Error ? err.message : '订单数据加载失败')
      })
      .finally(() => setLoading(false))
  }, [connectedGateway, start, end])

  useEffect(() => { fetchData() }, [fetchData])

  // WebSocket 有新的订单/成交推送时自动刷新
  const prevCount = useRef(wsOrderCount)
  useEffect(() => {
    if (wsOrderCount !== prevCount.current) {
      prevCount.current = wsOrderCount
      fetchData()
    }
  }, [wsOrderCount, fetchData])

  const pnlGroups = (pnl as Record<string, unknown>[]).reduce<Record<string, PnlGroup>>((acc, row) => {
    const symbol = row.symbol as string
    const group = acc[symbol] ?? { symbol, realized_pnl: 0, trade_count: 0, rows: [] }
    group.realized_pnl += Number(row.realized_pnl ?? 0)
    group.trade_count += 1
    group.rows.push(row)
    acc[symbol] = group
    return acc
  }, {})
  const pnlSummary = Object.values(pnlGroups).sort((a, b) => a.symbol.localeCompare(b.symbol))

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
          <a href={`/api/trades/export${rangeParams(connectedGateway, start, end)}`}
            className="ml-auto px-3 py-1.5 text-sm rounded hover:bg-[var(--bg-hover)]"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>
            导出CSV
          </a>
        )}
      </div>

      <div className="mb-3 flex flex-wrap items-end gap-3 rounded p-3" style={{ backgroundColor: 'var(--bg-surface)' }}>
        <label className="text-xs" style={{ color: 'var(--text-secondary)' }}>
          开始
          <input type="datetime-local" value={start} onChange={e => setStart(e.target.value)}
            className="ml-2 rounded px-2 py-1 text-sm"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-primary)', border: '1px solid var(--border)' }} />
        </label>
        <label className="text-xs" style={{ color: 'var(--text-secondary)' }}>
          结束
          <input type="datetime-local" value={end} onChange={e => setEnd(e.target.value)}
            className="ml-2 rounded px-2 py-1 text-sm"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-primary)', border: '1px solid var(--border)' }} />
        </label>
        {(start || end) && (
          <button onClick={() => { setStart(''); setEnd('') }}
            className="rounded px-3 py-1.5 text-xs"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>
            清除
          </button>
        )}
      </div>

      <div className="mb-3 rounded p-3" style={{ backgroundColor: 'var(--bg-surface)' }}>
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>Gateway</span>
          <span className="font-mono text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>
            {connectedGateway ?? '-'}
          </span>
          {accountIds.map(id => (
            <span key={id} className="rounded px-2 py-1 font-mono text-xs"
              style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-primary)' }}>
              {id}
            </span>
          ))}
        </div>
      </div>

      {error && (
        <div className="mb-3 rounded border px-3 py-2 text-sm" style={{ borderColor: '#d32f2f', color: '#d32f2f', backgroundColor: 'var(--bg-raised)' }}>
          {error}
        </div>
      )}

      {loading && (
        <div className="mb-3 text-sm" style={{ color: 'var(--text-secondary)' }}>加载中...</div>
      )}

      {tab === 'orders' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[550px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">下单时间</th><th className="text-left py-2 px-3">标的</th><th className="text-left py-2 px-3">方向</th>
              <th className="text-right py-2 px-3">数量</th><th className="text-right py-2 px-3">价格</th>
              <th className="text-left py-2 px-3">状态</th>
            </tr></thead>
            <tbody>{(orders as Record<string, unknown>[]).map((o, i) => (
              <tr key={i} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 text-xs" style={{ color: 'var(--text-secondary)' }}>{formatDateTime(o.created_at)}</td>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>
                  {o.symbol as string}
                </td>
                <td className="py-2 px-3" style={{ color: o.action === 'BUY' ? '#26a641' : '#d32f2f' }}>{o.action as string}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{o.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{formatNumber(o.limit_price, getSymbolDecimalPlaces(o.symbol as string))}</td>
                <td className="py-2 px-3" style={{ color: 'var(--text-secondary)' }}>{o.status as string}</td>
              </tr>
            ))}
              {!loading && orders.length === 0 && (
                <tr><td colSpan={6} className="py-6 text-center" style={{ color: 'var(--text-secondary)' }}>暂无订单数据</td></tr>
              )}
            </tbody>
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
                <td className="py-2 px-3 text-xs" style={{ color: 'var(--text-secondary)' }}>{formatDateTime(t.time)}</td>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>
                  {t.symbol as string}
                </td>
                <td className="py-2 px-3" style={{ color: t.side === 'BOT' ? '#26a641' : '#d32f2f' }}>{t.side as string}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{t.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{formatNumber(t.price, getSymbolDecimalPlaces(t.symbol as string))}</td>
                <td className="py-2 px-3 text-right" style={{ color: 'var(--text-secondary)' }}>{t.commission as number}</td>
              </tr>
            ))}
              {!loading && trades.length === 0 && (
                <tr><td colSpan={6} className="py-6 text-center" style={{ color: 'var(--text-secondary)' }}>暂无成交数据</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'pnl' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[900px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-right py-2 px-3">已实现盈亏</th>
              <th className="text-right py-2 px-3">平仓次数</th>
              <th className="text-left py-2 px-3">明细</th>
            </tr></thead>
            <tbody>{pnlSummary.map(group => (
              <tr key={group.symbol} className="border-b align-top" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 font-mono font-semibold" style={{ color: 'var(--text-primary)' }}>{group.symbol}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: group.realized_pnl >= 0 ? '#26a641' : '#d32f2f' }}>
                  {formatNumber(group.realized_pnl, 2)}
                </td>
                <td className="py-2 px-3 text-right" style={{ color: 'var(--text-secondary)' }}>{group.trade_count}</td>
                <td className="py-2 px-3">
                  <div className="space-y-1">
                    {group.rows.map((p, i) => (
                      <div key={i} className="grid grid-cols-[150px_70px_1fr_90px] gap-2 text-xs">
                        <span style={{ color: 'var(--text-secondary)' }}>{formatDateTime(p.time)}</span>
                        <span className="font-mono" style={{ color: 'var(--text-primary)' }}>{p.side as string}</span>
                        <span className="font-mono" style={{ color: 'var(--text-secondary)' }}>
                          {formatNumber(p.quantity, 2)} @ {formatNumber(p.entry_price, getSymbolDecimalPlaces(group.symbol))} {'->'} {formatNumber(p.exit_price, getSymbolDecimalPlaces(group.symbol))}
                        </span>
                        <span className="text-right font-mono" style={{ color: Number(p.realized_pnl ?? 0) >= 0 ? '#26a641' : '#d32f2f' }}>
                          {formatNumber(p.realized_pnl, 2)}
                        </span>
                      </div>
                    ))}
                  </div>
                </td>
              </tr>
            ))}
              {!loading && pnlSummary.length === 0 && (
                <tr><td colSpan={4} className="py-6 text-center" style={{ color: 'var(--text-secondary)' }}>暂无盈亏数据</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
