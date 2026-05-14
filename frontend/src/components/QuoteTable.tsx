import { useMarketStore, type Quote } from '../store/marketStore'
import { getSymbolDescription, getSymbolDecimalPlaces } from '../config/productConfig'

interface Props {
  onSelect: (symbol: string) => void
  activeSymbol: string | null
}

/** Format a price for a given symbol, treating null/-1 as no data */
function fmtPrice(val: number | null | undefined, symbol?: string): string {
  if (val == null || val === -1) return '-'
  return val.toFixed(getSymbolDecimalPlaces(symbol))
}

function fmtVolume(val: number | null | undefined): string {
  if (val == null || val === -1) return '-'
  return val.toLocaleString()
}

export function QuoteTable({ onSelect, activeSymbol }: Props) {
  const quotes = useMarketStore(s => s.quotes)
  const rows = Object.values(quotes)

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
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
            className="border-b cursor-pointer"
            style={{
              borderColor: 'var(--border-light)',
              backgroundColor: activeSymbol === q.symbol ? 'var(--bg-hover-light)' : undefined,
            }}
            onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--bg-hover-light)' }}
            onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = activeSymbol === q.symbol ? 'var(--bg-hover-light)' : '' }}
          >
            <td className="py-2 px-3 font-mono font-bold">
              {q.symbol}
              {getSymbolDescription(q.symbol) && (
                <span className="ml-1.5 font-normal" style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>
                  {getSymbolDescription(q.symbol)}
                </span>
              )}
            </td>
            <td className="py-2 px-3 text-right font-mono">{fmtPrice(q.last, q.symbol)}</td>
            <td className="py-2 px-3 text-right font-mono" style={{ color: '#2962ff' }}>{fmtPrice(q.bid, q.symbol)}</td>
            <td className="py-2 px-3 text-right font-mono" style={{ color: '#ff9800' }}>{fmtPrice(q.ask, q.symbol)}</td>
            <td className="py-2 px-3 text-right" style={{ color: 'var(--text-muted)' }}>{fmtVolume(q.volume)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
