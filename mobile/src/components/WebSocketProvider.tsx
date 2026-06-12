import { useMarketStore } from '../stores/marketStore'
import { useAccountStore } from '../stores/accountStore'
import { useOrderStore } from '../stores/orderStore'
import { useWebSocket } from '../hooks/useWebSocket'
import { getRuntimeConfig } from '../config/runtimeConfig'

function getWsBase() {
  return getRuntimeConfig().wsUrl
}

export function WebSocketProvider() {
  const updateQuote = useMarketStore(s => s.updateQuote)
  const updateTick = useMarketStore(s => s.updateTick)
  const updateFuturesRollState = useMarketStore(s => s.updateFuturesRollState)
  const setConnected = useMarketStore(s => s.setConnected)
  const setAccount = useAccountStore(s => s.setAccount)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  const addUpdate = useOrderStore(s => s.addUpdate)

  useWebSocket('/ws/market', (data: any) => {
    updateQuote(data)
  }, {
    onOpen: () => setConnected(true),
    onClose: () => setConnected(false),
  }, getWsBase())

  useWebSocket('/ws/tick', (data: any) => updateTick(data), undefined, getWsBase())
  useWebSocket('/ws/account', (data: any) => setAccount(data), undefined, getWsBase())
  useWebSocket('/ws/orders', (data: any) => addUpdate(data), undefined, getWsBase())
  useWebSocket('/ws/futures/roll-state', (data: any) => {
    if (data && typeof data === 'object' && data.symbol && data.active) {
      updateFuturesRollState(data)
    }
  }, undefined, getWsBase())
  useWebSocket('/ws/gateway/map', (data: any) => {
    if (data && typeof data === 'object') setGatewayMap(data)
  }, undefined, getWsBase())

  return null
}
