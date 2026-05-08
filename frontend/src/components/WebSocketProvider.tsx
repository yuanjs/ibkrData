import { useMarketStore } from '../store/marketStore'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'
import { useWebSocket } from '../hooks/useWebSocket'

/**
 * Global WebSocket connections — rendered once in App.tsx so that
 * the connected status and data flow work on every page.
 */
export function WebSocketProvider() {
  const updateQuote = useMarketStore(s => s.updateQuote)
  const updateTick = useMarketStore(s => s.updateTick)
  const setConnected = useMarketStore(s => s.setConnected)
  const setAccount = useAccountStore(s => s.setAccount)
  const addUpdate = useOrderStore(s => s.addUpdate)

  useWebSocket('/ws/market', (data: any) => {
    updateQuote(data)
  }, {
    onOpen: () => setConnected(true),
    onClose: () => setConnected(false),
  })

  useWebSocket('/ws/tick', (data: any) => updateTick(data))
  useWebSocket('/ws/account', (data: any) => setAccount(data))
  useWebSocket('/ws/orders', (data: any) => addUpdate(data))

  return null  // This component renders nothing — it just sets up WebSocket connections
}
