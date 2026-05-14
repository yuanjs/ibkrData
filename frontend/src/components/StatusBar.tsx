import { useMarketStore } from '../store/marketStore'

export function StatusBar() {
  const connected = useMarketStore(s => s.connected)
  return (
    <div className="flex items-center gap-2 px-4 py-2 text-sm" style={{ borderBottom: '1px solid var(--border)' }}>
      <span className="w-2 h-2 rounded-full" style={{ backgroundColor: connected ? '#089981' : '#f23645' }} />
      <span style={{ color: connected ? '#089981' : '#f23645' }}>
        {connected ? 'IBKR 已连接' : '断开连接 - 重连中...'}
      </span>
    </div>
  )
}
