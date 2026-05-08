import { useMarketStore } from '../store/marketStore'

export function StatusBar() {
  const connected = useMarketStore(s => s.connected)
  return (
    <div className="flex items-center gap-2 px-4 py-2 text-sm" style={{ borderBottom: '1px solid var(--border)' }}>
      <span className={`w-2 h-2 rounded-full ${connected ? 'bg-green-400' : 'bg-red-400'}`} />
      <span className={connected ? 'text-green-400' : 'text-red-400'}>
        {connected ? 'IBKR 已连接' : '断开连接 - 重连中...'}
      </span>
    </div>
  )
}
