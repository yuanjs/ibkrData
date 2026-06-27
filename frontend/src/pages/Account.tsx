import { useState, useEffect, useRef } from 'react'
import { api } from '../api/client'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'
import { useMarketStore } from '../store/marketStore'
import { getProductConfig, getSymbolDecimalPlaces } from '../config/productConfig'

interface EquityPoint {
  time: string
  net_liquidation: number
  daily_pnl?: number
}

export function Account() {
  const connectedGateway = useAccountStore(s => s.connectedGateway)
  const accountIds = useAccountStore(s => s.accountIds)
  const summary = useAccountStore(s => s.summary)
  const positions = useAccountStore(s => s.positions)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)
  const [closeMsg, setCloseMsg] = useState<string | null>(null)
  const [equityDays, setEquityDays] = useState(30)
  const [equityHistory, setEquityHistory] = useState<EquityPoint[]>([])

  // 加载 gateway map — 仅在 WebSocket 尚未推送时通过 HTTP 获取
  useEffect(() => {
    api.get<Record<string, unknown>>('/gateway/map').then(setGatewayMap).catch(() => {})
  }, [setGatewayMap])

  // 页面初始加载；gatewayMap 到达后自动按 collector 当前 gateway 重新加载
  useEffect(() => {
    const params = connectedGateway ? `?gateway=${connectedGateway}` : ''
    Promise.all([
      api.get<Record<string, unknown>[]>(`/account${params}`),
      api.get<Record<string, unknown>[]>(`/positions${params}`),
    ]).then(([accounts, positions]) => {
      if (Array.isArray(accounts) && accounts.length) {
        useAccountStore.getState().setAccount({
          accounts,
          positions: Array.isArray(positions) ? positions : [],
        })
      }
    }).catch(() => {})
  }, [connectedGateway])

  useEffect(() => {
    const end = new Date()
    const start = new Date(end.getTime() - equityDays * 24 * 60 * 60 * 1000)
    const params = new URLSearchParams({
      start: start.toISOString(),
      end: end.toISOString(),
    })
    if (connectedGateway) params.set('gateway', connectedGateway)
    api.get<EquityPoint[]>(`/account/history?${params.toString()}`)
      .then(rows => setEquityHistory(Array.isArray(rows) ? rows : []))
      .catch(() => setEquityHistory([]))
  }, [connectedGateway, equityDays])

  const fmt = (v: number | undefined) => v != null ? v.toLocaleString('en-US', { style: 'currency', currency: 'USD' }) : '-'
  const fmtPrice = (v: number | undefined, sym?: string) => {
    if (v == null) return '-'
    const d = sym ? getSymbolDecimalPlaces(sym) : 2
    return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d })
  }
  const pnlColor = (v: number | undefined) => v == null ? '' : v >= 0 ? '#26a641' : '#d32f2f'

  // ===== 实时 PnL：用 tick 价格推算持仓市值变化 =====
  // 用 ref 持有最新 quotes，避免每 1s tick 触发重渲染
  const quotes = useMarketStore(s => s.quotes)
  const quotesRef = useRef(quotes)
  quotesRef.current = quotes

  interface PnlRef {
    refPnl: number
    refMarketValue: number
    refPrice: number
  }
  const pnlRefs = useRef<Record<string, PnlRef>>({})

  // 每 3s 触发一次 PnL 显示刷新（不是每 1s tick）
  const [, setPnLTick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => setPnLTick(t => t + 1), 3000)
    return () => clearInterval(id)
  }, [])

  // 当前位置的快照行情（用于「当前报价」列 ref 更新）
  const prevPositionsRef = useRef('')
  const positionsKey = JSON.stringify((positions as Array<Record<string, unknown>>).map(p => [p.symbol, p.market_value, p.unrealized_pnl]))
  useEffect(() => {
    if (positionsKey === prevPositionsRef.current) return
    prevPositionsRef.current = positionsKey
    const refs: Record<string, PnlRef> = {}
    const liveQuotes = quotesRef.current
    for (const pos of (positions as Array<Record<string, unknown>>)) {
      const sym = pos.symbol as string
      const mv = pos.market_value as number | undefined
      const up = pos.unrealized_pnl as number | undefined
      const last = (liveQuotes as Record<string, any>)?.[sym]?.last
      if (mv != null && up != null && last != null && last > 0) {
        refs[sym] = { refPnl: up, refMarketValue: mv, refPrice: last }
      }
    }
    if (Object.keys(refs).length) pnlRefs.current = refs
  }, [positionsKey])  // 不再依赖 quotes — 用 ref 读取最新值

  function getQuote(sym: string) { return (quotesRef.current as Record<string, any>)?.[sym] }

  /**
   * 计算市值和 PnL。
   * ref — 有 IBKR 快照时用 ratio 实时推算
   * est — 无 IBKR 数据时，用 IBKR 提供的 multiplier + tick 价格
   * raw — 无任何数据，透传 IBKR 原始值（可能 null）
   */
  function calcMv(pos: Record<string, unknown>): { mv: number | undefined; src: 'ref' | 'est' | 'raw' } {
    const ref = pnlRefs.current[pos.symbol as string]
    if (ref) {
      const p = getQuote(pos.symbol as string)?.last
      if (p && p > 0 && ref.refPrice > 0)
        return { mv: ref.refMarketValue * (p / ref.refPrice), src: 'ref' }
      return { mv: undefined, src: 'raw' }
    }
    // 用 IBKR 提供的 multiplier 估值
    const last = getQuote(pos.symbol as string)?.last
    const qty = pos.quantity as number | undefined
    if (last != null && last > 0 && qty != null && qty !== 0) {
      return { mv: last * qty * getMult(pos), src: 'est' }
    }
    return { mv: pos.market_value as number | undefined, src: 'raw' }
  }

  function calcPnl(pos: Record<string, unknown>): { pnl: number | undefined; src: 'ref' | 'est' | 'raw' } {
    const ref = pnlRefs.current[pos.symbol as string]
    if (ref) {
      const p = getQuote(pos.symbol as string)?.last
      if (p && p > 0 && ref.refPrice > 0 && ref.refMarketValue) {
        const ratio = p / ref.refPrice
        return { pnl: ref.refPnl + (ref.refMarketValue * ratio - ref.refMarketValue), src: 'ref' }
      }
      return { pnl: undefined, src: 'raw' }
    }
    // 用 IBKR 提供的 multiplier + tick 价格
    const last = getQuote(pos.symbol as string)?.last
    const qty = pos.quantity as number | undefined
    const avg = pos.avg_cost as number | undefined
    if (last != null && last > 0 && qty != null && qty !== 0 && avg != null && avg > 0) {
      const mult = getMult(pos)
      // avgCost 已含乘数: PnL = qty × (last × mult - avg)
      return { pnl: (last * mult - avg) * qty, src: 'est' }
    }
    return { pnl: pos.unrealized_pnl as number | undefined, src: 'raw' }
  }

  /** 取合约乘数：优先用 IBKR 实时数据，回退到 productConfig */
  function getMult(pos: Record<string, unknown>): number {
    const fromPos = pos.multiplier as number | undefined
    if (fromPos != null && fromPos > 0) return fromPos
    return getProductConfig(pos.symbol as string).multiplier ?? 1
  }

  /** 开仓价：avgCost ÷ multiplier（还原为产品报价，如指数点数） */
  function entryPrice(pos: Record<string, unknown>): string {
    const avg = pos.avg_cost as number | undefined
    if (avg == null) return '-'
    return fmtPrice(avg / getMult(pos), pos.symbol as string)
  }

  const RealtimeBadge = () => (
    <span style={{ fontSize: '0.6rem', color: 'var(--text-secondary)', marginLeft: 3, verticalAlign: 'super' }}>⚡</span>
  )

  const EquityChart = () => {
    const width = 720
    const height = 180
    const pad = 24
    const validRows = equityHistory.filter(p => Number.isFinite(Number(p.net_liquidation)))
    const stride = Math.max(1, Math.ceil(validRows.length / 800))
    const rows = validRows.filter((_, index) => index % stride === 0 || index === validRows.length - 1)
    if (rows.length < 2) {
      return (
        <div className="h-[180px] flex items-center justify-center text-sm" style={{ color: 'var(--text-secondary)' }}>
          暂无资金曲线数据
        </div>
      )
    }
    let min = Number(rows[0].net_liquidation)
    let max = min
    for (const row of rows) {
      const value = Number(row.net_liquidation)
      if (value < min) min = value
      if (value > max) max = value
    }
    const span = max - min || 1
    const points = rows.map((p, i) => {
      const x = pad + (i / (rows.length - 1)) * (width - pad * 2)
      const y = pad + ((max - Number(p.net_liquidation)) / span) * (height - pad * 2)
      return `${x},${y}`
    }).join(' ')
    const first = rows[0]
    const last = rows[rows.length - 1]
    const change = Number(last.net_liquidation) - Number(first.net_liquidation)

    return (
      <div>
        <div className="mb-2 flex items-center justify-between text-xs" style={{ color: 'var(--text-secondary)' }}>
          <span>{new Date(first.time).toLocaleDateString()} - {new Date(last.time).toLocaleDateString()}</span>
          <span className="font-mono" style={{ color: change >= 0 ? '#26a641' : '#d32f2f' }}>
            {fmt(change)}
          </span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="h-[180px] w-full" role="img" aria-label="账户资金曲线">
          <line x1={pad} y1={pad} x2={pad} y2={height - pad} stroke="var(--border)" />
          <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} stroke="var(--border)" />
          <polyline points={points} fill="none" stroke="#1a7f64" strokeWidth="2.5" vectorEffect="non-scaling-stroke" />
          <text x={pad} y={16} fill="var(--text-secondary)" fontSize="11">{fmt(max)}</text>
          <text x={pad} y={height - 6} fill="var(--text-secondary)" fontSize="11">{fmt(min)}</text>
        </svg>
      </div>
    )
  }

  // Watch for close order result via WebSocket
  useEffect(() => {
    if (!closePending) return
    const lastOrder = orders[0] as Record<string, unknown> | undefined
    if (lastOrder?.close_id === closePending.closeId) {
      const s = lastOrder?.status as string
      if (s === 'Filled') {
        setCloseMsg(`${closePending.symbol} 平仓成功 🎉`)
      } else if (s === 'Rejected' || s === 'Cancelled' || s === 'Inactive') {
        setCloseMsg(`${closePending.symbol} 平仓失败${s === 'Cancelled' ? ' (已取消)' : ''}`)
      }
      const timer = setTimeout(() => { setClosePending(null); setCloseMsg(null) }, 4000)
      return () => clearTimeout(timer)
    }
    // 超时自动清除（30 秒未匹配到 close_id）
    const timer = setTimeout(() => {
      setCloseMsg(`${closePending.symbol} 平仓超时，请检查订单`)
      setTimeout(() => { setClosePending(null); setCloseMsg(null) }, 4000)
    }, 30000)
    return () => clearTimeout(timer)
  }, [orders, closePending])

  const handleClose = async (symbol: string) => {
    const pos = (positions as Array<Record<string, unknown>>).find(p => p.symbol === symbol)
    if (!pos) return
    const sideLabel = (pos.quantity as number) > 0 ? '卖出' : '买入'
    const qty = Math.abs(pos.quantity as number)
    if (!window.confirm(`确定以市价平仓 ${symbol}？\n方向: ${sideLabel}\n数量: ${qty}`)) return

    try {
      setCloseMsg(null)
      const res = await api.post<{ close_id: string }>('/positions/close', {
        symbol,
        gateway: connectedGateway ?? 'live',
      })
      setClosePending({ closeId: res.close_id, symbol })
      setCloseMsg(`平仓指令已发送: ${symbol}`)
    } catch (e: any) {
      setCloseMsg(`平仓失败: ${e.message}`)
    }
  }

  return (
    <div className="p-4 space-y-6">
      <div className="rounded-lg p-4" style={{ backgroundColor: 'var(--bg-surface)' }}>
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>Gateway</span>
          <span className="font-mono text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>
            {connectedGateway ?? '-'}
          </span>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          {accountIds.length > 0 ? accountIds.map(id => (
            <span key={id} className="rounded px-2 py-1 font-mono text-xs"
              style={{ backgroundColor: 'var(--bg-raised)', color: 'var(--text-primary)' }}>
              {id}
            </span>
          )) : (
            <span className="text-sm" style={{ color: 'var(--text-secondary)' }}>暂无 account_id</span>
          )}
        </div>
      </div>

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

      <div className="rounded-lg p-4" style={{ backgroundColor: 'var(--bg-surface)' }}>
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-sm" style={{ color: 'var(--text-secondary)' }}>账户资金曲线</h2>
          <div className="flex gap-2">
            {[7, 30, 90].map(days => (
              <button key={days} onClick={() => setEquityDays(days)}
                className={`rounded px-3 py-1 text-xs ${equityDays === days ? 'bg-blue-600 text-white' : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'}`}>
                {days}天
              </button>
            ))}
          </div>
        </div>
        <EquityChart />
      </div>

      <div className="overflow-x-auto">
        <h2 className="text-sm mb-2" style={{ color: 'var(--text-secondary)' }}>当前持仓</h2>
        <table className="w-full text-sm min-w-[820px] md:min-w-0">
          <thead>
            <tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-center py-2 px-3">方向</th>
              <th className="text-right py-2 px-3">数量</th>
              <th className="text-right py-2 px-3">开仓价</th>
              <th className="text-right py-2 px-3">当前报价</th>
              <th className="text-right py-2 px-3">市值</th>
              <th className="text-right py-2 px-3">未实现盈亏</th>
              <th className="text-center py-2 px-3">操作</th>
            </tr>
          </thead>
          <tbody>
            {(positions as Array<Record<string, unknown>>).map((p) => {
              const sym = p.symbol as string
              const isPending = closePending?.symbol === sym
              return (
                <tr key={sym} className="border-b" style={{
                  borderColor: 'var(--border-light)',
                  opacity: isPending ? 0.6 : 1,
                }}>
                  <td className="py-2 px-3 font-mono font-bold" style={{ color: 'var(--text-primary)' }}>{sym}</td>
                  <td className="py-2 px-3 text-center font-mono text-xs font-semibold" style={{
                    color: (p.quantity as number) > 0 ? '#26a641' : (p.quantity as number) < 0 ? '#d32f2f' : 'var(--text-secondary)',
                  }}>
                    {(p.quantity as number) > 0 ? '多' : (p.quantity as number) < 0 ? '空' : '-'}
                  </td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{Math.abs(p.quantity as number)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{entryPrice(p)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>
                    {(() => {
                      const q = getQuote(sym)
                      if (q?.bid != null && q.bid > 0 && q?.ask != null && q.ask > 0) {
                        return `${fmtPrice(q.bid, sym)} / ${fmtPrice(q.ask, sym)}`
                      }
                      if (q?.last != null && q.last > 0) return fmtPrice(q.last, sym)
                      return '-'
                    })()}
                  </td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>
                    {(() => { const r = calcMv(p); return r.mv != null ? fmt(r.mv) : '-'; })()}
                    {calcMv(p).src !== 'raw' && <RealtimeBadge />}
                  </td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: pnlColor(calcPnl(p).pnl) }}>
                    {(() => { const r = calcPnl(p); return r.pnl != null ? fmt(r.pnl) : '-'; })()}
                    {calcPnl(p).src !== 'raw' && <RealtimeBadge />}
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
