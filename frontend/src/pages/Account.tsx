import { useAccountStore } from '../store/accountStore'

export function Account() {
  const summary = useAccountStore(s => s.summary) as Record<string, number>
  const positions = useAccountStore(s => s.positions) as Array<Record<string, unknown>>

  const fmt = (v: number | undefined) => v != null ? v.toLocaleString('en-US', { style: 'currency', currency: 'USD' }) : '-'
  const pnlColor = (v: number | undefined) => v == null ? '' : v >= 0 ? 'text-green-400' : 'text-red-400'

  return (
    <div className="p-4 space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: '净值', key: 'net_liquidation' },
          { label: '现金余额', key: 'total_cash' },
          { label: '可用资金', key: 'available_funds' },
          { label: '今日盈亏', key: 'daily_pnl' },
        ].map(({ label, key }) => (
          <div key={key} className="bg-gray-800 rounded-lg p-4">
            <div className="text-gray-400 text-xs mb-1">{label}</div>
            <div className={`text-lg font-mono font-bold ${key.includes('pnl') ? pnlColor(summary[key]) : ''}`}>
              {fmt(summary[key])}
            </div>
          </div>
        ))}
      </div>

      <div className="overflow-x-auto">
        <h2 className="text-gray-400 text-sm mb-2">当前持仓</h2>
        <table className="w-full text-sm min-w-[600px] md:min-w-0">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-right py-2 px-3">数量</th>
              <th className="text-right py-2 px-3">均价</th>
              <th className="text-right py-2 px-3">市值</th>
              <th className="text-right py-2 px-3">未实现盈亏</th>
            </tr>
          </thead>
          <tbody>
            {positions.map((p, i) => (
              <tr key={i} className="border-b border-gray-800">
                <td className="py-2 px-3 font-mono font-bold">{p.symbol as string}</td>
                <td className="py-2 px-3 text-right font-mono">{p.quantity as number}</td>
                <td className="py-2 px-3 text-right font-mono">{fmt(p.avg_cost as number)}</td>
                <td className="py-2 px-3 text-right font-mono">{fmt(p.market_value as number)}</td>
                <td className={`py-2 px-3 text-right font-mono ${pnlColor(p.unrealized_pnl as number)}`}>
                  {fmt(p.unrealized_pnl as number)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
