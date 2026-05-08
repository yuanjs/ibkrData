import { useMarketStore, type Quote } from '../store/marketStore'

interface Props {
  onSelect: (symbol: string) => void
}

/** Format a price, treating null/-1 as no data */
function fmtPrice(val: number | null | undefined): string {
  if (val == null || val === -1) return '-'
  return val.toFixed(2)
}

function fmtVolume(val: number | null | undefined): string {
  if (val == null || val === -1) return '-'
  return val.toLocaleString()
}

export function QuoteTable({ onSelect }: Props) {
  const quotes = useMarketStore(s => s.quotes)
  const rows = Object.values(quotes)

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-gray-400 border-b border-gray-700">
          <th className="text-left py-2 px-3">标的</th>
          <th className="text-right py-2 px-3">最新</th>
          <th className="text-right py-2 px-3">买价</th>
          <th className="text-right py-2 px-3">卖价</th>
          <th className="text-right py-2 px-3">成交量</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((q: Quote) => (
          <tr
            key={q.symbol}
            onClick={() => onSelect(q.symbol)}
            className="border-b border-gray-800 cursor-pointer hover:bg-gray-800"
          >
            <td className="py-2 px-3 font-mono font-bold">{q.symbol}</td>
            <td className="py-2 px-3 text-right font-mono">{fmtPrice(q.last)}</td>
            <td className="py-2 px-3 text-right font-mono text-blue-300">{fmtPrice(q.bid)}</td>
            <td className="py-2 px-3 text-right font-mono text-orange-300">{fmtPrice(q.ask)}</td>
            <td className="py-2 px-3 text-right text-gray-400">{fmtVolume(q.volume)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
