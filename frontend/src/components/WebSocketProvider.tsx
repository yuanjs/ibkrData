import { useMarketStore } from '../store/marketStore'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'
import { useWebSocket } from '../hooks/useWebSocket'

const isRecord = (value: unknown): value is Record<string, unknown> =>
  Boolean(value) && typeof value === 'object' && !Array.isArray(value)

/**
 * Global WebSocket connections — rendered once in App.tsx so that
 * the connected status and data flow work on every page.
 */
export function WebSocketProvider() {
  const updateQuote = useMarketStore(s => s.updateQuote)
  const updateTick = useMarketStore(s => s.updateTick)
  const updateFuturesRollState = useMarketStore(s => s.updateFuturesRollState)
  const setConnected = useMarketStore(s => s.setConnected)
  const setAccount = useAccountStore(s => s.setAccount)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const addUpdate = useOrderStore(s => s.addUpdate)

  useWebSocket('/ws/market', (data: unknown) => {
    if (isRecord(data) && typeof data.symbol === 'string') updateQuote(data as any)
  }, {
    onOpen: () => setConnected(true),
    onClose: () => setConnected(false),
  })

  useWebSocket('/ws/tick', (data: unknown) => {
    if (isRecord(data) && typeof data.symbol === 'string') updateTick(data as any)
  })
  useWebSocket('/ws/account', (data: unknown) => {
    if (isRecord(data)) setAccount(data)
  })
  useWebSocket('/ws/orders', (data: unknown) => addUpdate(data))
  useWebSocket('/ws/futures/roll-state', (data: unknown) => {
    if (isRecord(data) && data.symbol && data.active) {
      updateFuturesRollState(data as any)
    }
  })
  useWebSocket('/ws/gateway/map', (data: unknown) => {
    if (isRecord(data)) setGatewayMap(data)
  })

  return null  // This component renders nothing — it just sets up WebSocket connections
}
