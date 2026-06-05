import { create } from 'zustand'

interface GatewayData {
  summary: Record<string, unknown>
  positions: unknown[]
}

interface AccountStore {
  live: GatewayData
  paper: GatewayData
  activeGateway: 'live' | 'paper'
  gatewayMap: Record<string, string>
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
    const result: Record<string, unknown> = {}

    const liveAccs = accounts.filter(a => state.gatewayMap[a.account_id as string] === 'live')
    const paperAccs = accounts.filter(a => state.gatewayMap[a.account_id as string] === 'paper')
    const livePos = positions.filter(p => state.gatewayMap[p.account_id as string] === 'live')
    const paperPos = positions.filter(p => state.gatewayMap[p.account_id as string] === 'paper')

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
