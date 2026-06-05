import { create } from 'zustand'

interface GatewayData {
  summary: Record<string, unknown>
  positions: unknown[]
}

interface AccountStore {
  live: GatewayData
  paper: GatewayData
  activeGateway: 'live' | 'paper'
  gatewayMap: Record<string, string>  // account_id → 'live' | 'paper'
  hasPaper: boolean
  setAccount: (data: { accounts: Record<string, unknown>[]; positions: Record<string, unknown>[] }) => void
  setActiveGateway: (g: 'live' | 'paper') => void
  setGatewayMap: (map: Record<string, string[]>) => void
}

export const useAccountStore = create<AccountStore>((set) => ({
  live: { summary: {}, positions: [] },
  paper: { summary: {}, positions: [] },
  activeGateway: 'live',
  gatewayMap: {},
  hasPaper: false,

  setAccount: (data) => set(state => {
    const { accounts, positions } = data
    const gwMap = state.gatewayMap

    if (Object.keys(gwMap).length === 0) {
      // gatewayMap 未到：只存 summary 用于显示数值，positions 等 WebSocket 推送
      return {
        live: { summary: (accounts[0] as Record<string, unknown>) ?? {}, positions: state.live.positions },
      }
    }

    const result: Record<string, unknown> = {}
    const liveAccs = accounts.filter(a => gwMap[a.account_id as string] === 'live')
    const paperAccs = accounts.filter(a => gwMap[a.account_id as string] === 'paper')
    const livePos = positions.filter(p => gwMap[p.account_id as string] === 'live')
    const paperPos = positions.filter(p => gwMap[p.account_id as string] === 'paper')

    if (liveAccs.length) result.live = { summary: liveAccs[0], positions: state.live.positions }
    if (paperAccs.length) result.paper = { summary: paperAccs[0], positions: state.paper.positions }
    if (livePos.length) result.live = { ...(result.live as GatewayData ?? state.live), positions: livePos }
    if (paperPos.length) result.paper = { ...(result.paper as GatewayData ?? state.paper), positions: paperPos }

    return result
  }),

  setActiveGateway: (g) => set({ activeGateway: g }),

  setGatewayMap: (map) => set({
    gatewayMap: Object.entries(map).reduce((acc, [gw, ids]) => {
      ;(ids as string[]).filter(id => id !== 'All').forEach(id => { acc[id] = gw })
      return acc
    }, {} as Record<string, string>),
    hasPaper: Boolean((map.paper as string[] | undefined)?.length),
  }),
}))
