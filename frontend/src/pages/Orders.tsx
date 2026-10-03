import { Fragment, useCallback, useEffect, useState, useMemo } from 'react'
import { api } from '../api/client'
import { getSymbolDecimalPlaces } from '../config/productConfig'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'
import { useMarketStore, type Quote } from '../store/marketStore'

const formatNumber = (value: unknown, decimals?: number, fallback = '-') => {
  const num = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(num) ? num.toFixed(decimals) : fallback
}

const formatDateTime = (value: unknown) => value ? new Date(value as string).toLocaleString() : '-'

const quotePrice = (quote?: Quote) => {
  if (quote?.last != null && quote.last > 0) return quote.last
  if (quote?.bid != null && quote.bid > 0 && quote?.ask != null && quote.ask > 0) {
    return (quote.bid + quote.ask) / 2
  }
  return quote?.bid != null && quote.bid > 0 ? quote.bid : quote?.ask != null && quote.ask > 0 ? quote.ask : null
}

const convertPnlToAud = (value: number, currency: string, audUsdRate: number | null, usdJpyRate: number | null) => {
  switch (currency.toUpperCase()) {
    case 'AUD': return value
    case 'USD': return audUsdRate ? value / audUsdRate : null
    case 'JPY': return audUsdRate && usdJpyRate ? value / (audUsdRate * usdJpyRate) : null
    default: return null
  }
}

const toApiDate = (value: string) => value ? new Date(value).toISOString() : ''

type RangePreset = '24h' | '3d' | '7d' | '1m' | 'custom'

const RANGE_PRESETS: { key: RangePreset; label: string }[] = [
  { key: '24h', label: '24小时' },
  { key: '3d', label: '3天' },
  { key: '7d', label: '7天' },
  { key: '1m', label: '1个月' },
  { key: 'custom', label: '自定义' },
]

const toLocalDateTime = (date: Date) => {
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`
}

const presetRange = (preset: Exclude<RangePreset, 'custom'>) => {
  const end = new Date()
  const start = new Date(end)
  if (preset === '1m') {
    const day = start.getDate()
    start.setDate(1)
    start.setMonth(start.getMonth() - 1)
    const lastDay = new Date(start.getFullYear(), start.getMonth() + 1, 0).getDate()
    start.setDate(Math.min(day, lastDay))
  }
  else start.setHours(start.getHours() - (preset === '24h' ? 24 : preset === '3d' ? 72 : 168))
  return { start: toLocalDateTime(start), end: toLocalDateTime(end) }
}

const rangeParams = (gateway: string | null, start: string, end: string) => {
  const params = new URLSearchParams()
  if (gateway) params.set('gateway', gateway)
  if (start) params.set('start', toApiDate(start))
  if (end) params.set('end', toApiDate(end))
  const text = params.toString()
  return text ? `?${text}` : ''
}

type PnlGroup = {
  key: string
  symbol: string
  currency: string
  realized_pnl: number
  realized_pnl_aud: number | null
  trade_count: number
  rows: Record<string, unknown>[]
}

type TradeGroup = {
  key: string
  order_id: string
  time: unknown
  symbol: string
  side: string
  quantity: number
  average_price: number
  commission: number
  rows: Record<string, unknown>[]
}

export function Orders() {
  const [initialRange] = useState(() => presetRange('24h'))
  const [orders, setOrders] = useState<unknown[]>([])
  const [trades, setTrades] = useState<unknown[]>([])
  const [tab, setTab] = useState<'orders' | 'trades' | 'pnl'>('orders')
  const [pnl, setPnl] = useState<unknown[]>([])
  const [rangePreset, setRangePreset] = useState<RangePreset>('24h')
  const [start, setStart] = useState(initialRange.start)
  const [end, setEnd] = useState(initialRange.end)
  const [appliedStart, setAppliedStart] = useState(initialRange.start)
  const [appliedEnd, setAppliedEnd] = useState(initialRange.end)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [expandedSymbols, setExpandedSymbols] = useState<Set<string>>(() => new Set())
  const [expandedTradeOrders, setExpandedTradeOrders] = useState<Set<string>>(() => new Set())

  const connectedGateway = useAccountStore(s => s.connectedGateway)
  const accountIds = useAccountStore(s => s.accountIds)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const wsOrderCount = useOrderStore(s => s.orders.length)
  const audUsdQuote = useMarketStore(s => s.quotes['AUD.USD'] ?? s.quotes.AUDUSD)
  const usdJpyQuote = useMarketStore(s => s.quotes['USD.JPY'] ?? s.quotes.USDJPY)
  const audUsdRate = quotePrice(audUsdQuote)
  const usdJpyRate = quotePrice(usdJpyQuote)

  // 页面刷新后通过 REST 加载 gateway map（不等 WebSocket）
  useEffect(() => {
    api.get<Record<string, unknown>>('/gateway/map').then(setGatewayMap).catch(() => {})
  }, [setGatewayMap])

  const fetchData = useCallback(() => {
    const controller = new AbortController()
    const range = rangePreset === 'custom' ? { start: appliedStart, end: appliedEnd } : presetRange(rangePreset)
    const params = rangeParams(connectedGateway, range.start, range.end)
    const endpoint = tab === 'orders' ? '/orders' : tab === 'trades' ? '/trades' : '/pnl'
    setLoading(true)
    setError(null)
    api.get(`${endpoint}${params}`, { signal: controller.signal })
      .then(data => {
        const rows = Array.isArray(data) ? data : []
        if (tab === 'orders') setOrders(rows)
        else if (tab === 'trades') setTrades(rows)
        else setPnl(rows)
      })
      .catch(err => {
        if (err instanceof DOMException && err.name === 'AbortError') return
        setError(err instanceof Error ? err.message : '订单数据加载失败')
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [connectedGateway, appliedStart, appliedEnd, rangePreset, tab])

  // WebSocket 有新的订单/成交推送时自动刷新
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
    const groups = (pnl as Record<string, unknown>[]).reduce<Record<string, PnlGroup>>((acc, row) => {
      const symbol = row.symbol as string
      const currency = String(row.currency ?? '')
      const contractIdentity = `${row.con_id ?? ''}:${row.local_symbol ?? ''}`
      const key = `${symbol}:${currency}:${contractIdentity}`
      const group = acc[key] ?? { key, symbol, currency, realized_pnl: 0, realized_pnl_aud: null, trade_count: 0, rows: [] }
      group.realized_pnl += Number(row.realized_pnl ?? 0)
      group.trade_count += 1
      group.rows.push(row)
      acc[key] = group
      return acc
    }, {})
    return Object.values(groups)
      .map(group => ({
        ...group,
        realized_pnl_aud: convertPnlToAud(group.realized_pnl, group.currency, audUsdRate, usdJpyRate),
      }))
      .sort((a, b) => a.symbol.localeCompare(b.symbol))
  }, [audUsdRate, pnl, usdJpyRate])

  const tradeSummary = useMemo(() => {
    const groups = new Map<string, TradeGroup & { notional: number }>()
    for (const row of trades as Record<string, unknown>[]) {
      const orderId = row.order_id == null ? '-' : String(row.order_id)
      const symbol = String(row.symbol ?? '-')
      const side = String(row.side ?? '-')
      const identity = row.order_id == null
        ? String(row.exec_id ?? `${symbol}:${side}:${row.time}`)
        : `${row.account_id ?? ''}:${orderId}:${row.con_id ?? ''}:${row.local_symbol ?? ''}:${side}`
      const quantity = Number(row.quantity ?? 0)
      const price = Number(row.price ?? 0)
      const group = groups.get(identity) ?? {
        key: identity,
        order_id: orderId,
        time: row.time,
        symbol,
        side,
        quantity: 0,
        average_price: 0,
        commission: 0,
        notional: 0,
        rows: [],
      }
      group.quantity += quantity
      group.notional += quantity * price
      group.commission += Number(row.commission ?? 0)
      group.rows.push(row)
      groups.set(identity, group)
    }
    return [...groups.values()].map(({ notional, ...group }) => ({
      ...group,
      average_price: group.quantity ? notional / group.quantity : 0,
    }))
  }, [trades])

  const applyRange = () => {
    setAppliedStart(start)
    setAppliedEnd(end)
  }

  const selectPreset = (preset: RangePreset) => {
    setRangePreset(preset)
    if (preset !== 'custom') {
      const range = presetRange(preset)
      setStart(range.start)
      setEnd(range.end)
      setAppliedStart(range.start)
      setAppliedEnd(range.end)
    }
  }

  const exportRange = rangePreset === 'custom' ? { start: appliedStart, end: appliedEnd } : presetRange(rangePreset)
  const invalidCustomRange = !start || !end || new Date(start) > new Date(end)

  const toggleSymbol = (key: string) => {
    setExpandedSymbols(current => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const toggleTradeOrder = (key: string) => {
    setExpandedTradeOrders(current => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

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
          <a href={`/api/trades/export${rangeParams(connectedGateway, exportRange.start, exportRange.end)}`}
            className="ml-auto px-3 py-1.5 text-sm rounded hover:bg-[var(--bg-hover)]"
            style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>
            导出CSV
          </a>
        )}
      </div>

      <div className="mb-3 flex flex-wrap items-end gap-3 rounded p-3" style={{ backgroundColor: 'var(--bg-surface)' }}>
        <div className="flex flex-wrap gap-2">
          {RANGE_PRESETS.map(option => (
            <button key={option.key} onClick={() => selectPreset(option.key)}
              className={`rounded px-3 py-1.5 text-xs ${rangePreset === option.key ? 'bg-blue-600 text-white' : ''}`}
              style={rangePreset === option.key ? undefined : { backgroundColor: 'var(--bg-raised)', color: 'var(--text-secondary)' }}>
              {option.label}
            </button>
          ))}
        </div>
        {rangePreset === 'custom' && <>
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
          <button onClick={applyRange} disabled={invalidCustomRange || (start === appliedStart && end === appliedEnd)}
            className="rounded bg-blue-600 px-3 py-1.5 text-xs text-white disabled:cursor-not-allowed disabled:opacity-50">
            查询
          </button>
          {start && end && new Date(start) > new Date(end) && (
            <span className="self-center text-xs" style={{ color: '#d32f2f' }}>开始时间不能晚于结束时间</span>
          )}
        </>}
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
          <table className="w-full text-sm min-w-[850px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">订单</th><th className="text-left py-2 px-3">时间</th>
              <th className="text-left py-2 px-3">标的</th><th className="text-left py-2 px-3">方向</th>
              <th className="text-right py-2 px-3">成交数量</th><th className="text-right py-2 px-3">成交均价</th>
              <th className="text-right py-2 px-3">手续费</th><th className="text-left py-2 px-3">明细</th>
            </tr></thead>
            <tbody>{tradeSummary.map(group => (
              <Fragment key={group.key}>
                <tr className="border-b" style={{ borderColor: 'var(--border-light)' }}>
                  <td className="py-2 px-3 font-mono text-xs" style={{ color: 'var(--text-secondary)' }}>{group.order_id}</td>
                  <td className="py-2 px-3 text-xs" style={{ color: 'var(--text-secondary)' }}>{formatDateTime(group.time)}</td>
                  <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>{group.symbol}</td>
                  <td className="py-2 px-3" style={{ color: group.side === 'BOT' ? '#26a641' : '#d32f2f' }}>{group.side}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{formatNumber(group.quantity, 2)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{formatNumber(group.average_price, getSymbolDecimalPlaces(group.symbol))}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-secondary)' }}>{formatNumber(group.commission, 2)}</td>
                  <td className="py-2 px-3">
                    <button onClick={() => toggleTradeOrder(group.key)} className="text-xs text-blue-500 hover:underline">
                      {expandedTradeOrders.has(group.key) ? '收起明细' : `展开 ${group.rows.length} 笔`}
                    </button>
                  </td>
                </tr>
                {expandedTradeOrders.has(group.key) && (
                  <tr key={`${group.key}:details`} className="border-b" style={{ borderColor: 'var(--border-light)', backgroundColor: 'var(--bg-surface)' }}>
                    <td colSpan={8} className="px-6 py-3">
                      <div className="grid grid-cols-[180px_100px_120px_100px] gap-3 text-xs" style={{ color: 'var(--text-secondary)' }}>
                        <span>成交时间</span><span className="text-right">数量</span><span className="text-right">价格</span><span className="text-right">手续费</span>
                        {group.rows.map((fill, i) => (
                          <div key={String(fill.exec_id ?? i)} className="contents">
                            <span>{formatDateTime(fill.time)}</span>
                            <span className="text-right font-mono">{formatNumber(fill.quantity, 2)}</span>
                            <span className="text-right font-mono">{formatNumber(fill.price, getSymbolDecimalPlaces(group.symbol))}</span>
                            <span className="text-right font-mono">{formatNumber(fill.commission, 2)}</span>
                          </div>
                        ))}
                      </div>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
              {!loading && tradeSummary.length === 0 && (
                <tr><td colSpan={8} className="py-6 text-center" style={{ color: 'var(--text-secondary)' }}>暂无成交数据</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'pnl' && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm min-w-[1000px] md:min-w-0">
            <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-left py-2 px-3">币种</th>
              <th className="text-right py-2 px-3">已实现盈亏</th>
              <th className="text-right py-2 px-3">AUD盈亏</th>
              <th className="text-right py-2 px-3">平仓次数</th>
              <th className="text-left py-2 px-3">明细</th>
            </tr></thead>
            <tbody>{pnlSummary.map(group => (
              <tr key={group.key} className="border-b align-top" style={{ borderColor: 'var(--border-light)' }}>
                <td className="py-2 px-3 font-mono font-semibold" style={{ color: 'var(--text-primary)' }}>{group.symbol}</td>
                <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-secondary)' }}>{group.currency || '-'}</td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: group.realized_pnl >= 0 ? '#26a641' : '#d32f2f' }}>
                  {formatNumber(group.realized_pnl, 2)}
                </td>
                <td className="py-2 px-3 text-right font-mono" style={{ color: group.realized_pnl_aud == null ? 'var(--text-muted)' : group.realized_pnl_aud >= 0 ? '#26a641' : '#d32f2f' }}>
                  {group.realized_pnl_aud == null ? '-' : formatNumber(group.realized_pnl_aud, 2)}
                </td>
                <td className="py-2 px-3 text-right" style={{ color: 'var(--text-secondary)' }}>{group.trade_count}</td>
                <td className="py-2 px-3">
                  <button onClick={() => toggleSymbol(group.key)} className="mb-1 text-xs text-blue-500 hover:underline">
                    {expandedSymbols.has(group.key) ? '收起明细' : `展开 ${group.rows.length} 条明细`}
                  </button>
                  {expandedSymbols.has(group.key) && <div className="space-y-1">
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
                  </div>}
                </td>
              </tr>
            ))}
              {!loading && pnlSummary.length === 0 && (
                <tr><td colSpan={6} className="py-6 text-center" style={{ color: 'var(--text-secondary)' }}>暂无盈亏数据</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
