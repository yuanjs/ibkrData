import { create } from 'zustand'

interface AccountStore {
  summary: Record<string, unknown>
  positions: unknown[]
  accountIds: string[]
  connectedGateway: string | null
  gatewayMap: Record<string, string>  // account_id → 'live' | 'paper'
  setAccount: (data: { accounts?: Record<string, unknown>[]; positions?: Record<string, unknown>[] }) => void
  setGatewayMap: (map: Record<string, unknown>) => void
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  Boolean(value) && typeof value === 'object' && !Array.isArray(value)

function normalizeGatewayMap(map: Record<string, unknown>) {
  const grouped: Record<string, string[]> = {}
  for (const [key, value] of Object.entries(map)) {
    if (Array.isArray(value)) {
      grouped[key] = value.filter((id): id is string => typeof id === 'string' && id !== 'All' && id !== '')
    } else if (typeof value === 'string' && key !== 'All' && key !== '') {
      grouped[value] = [...(grouped[value] ?? []), key]
    }
  }
  return Object.entries(grouped)
    .map(([gateway, ids]) => ({ gateway, ids: Array.from(new Set(ids)) }))
    .filter(entry => entry.ids.length > 0)
}

const sameStringArray = (a: string[], b: string[]) =>
  a.length === b.length && a.every((value, index) => value === b[index])

const sameGatewayMap = (a: Record<string, string>, b: Record<string, string>) => {
  const aKeys = Object.keys(a).sort()
  const bKeys = Object.keys(b).sort()
  return sameStringArray(aKeys, bKeys) && aKeys.every(key => a[key] === b[key])
}

export const useAccountStore = create<AccountStore>((set) => ({
  summary: {},
  positions: [],
  accountIds: [],
  connectedGateway: null,
  gatewayMap: {},

  setAccount: (data) => set(state => {
    const accounts = Array.isArray(data?.accounts) ? data.accounts.filter(isRecord) : []
    const positions = Array.isArray(data?.positions) ? data.positions.filter(isRecord) : []
    const allowedIds = state.accountIds.length ? new Set(state.accountIds) : null
    const visibleAccounts = allowedIds
      ? accounts.filter(a => allowedIds.has(a.account_id as string))
      : accounts.filter(a => a.account_id !== 'All')
    const visiblePositions = allowedIds
      ? positions.filter(p => allowedIds.has(p.account_id as string))
      : positions

    return {
      summary: visibleAccounts[0] ?? state.summary,
      positions: positions.length ? visiblePositions : state.positions,
      accountIds: state.accountIds.length
        ? state.accountIds
        : Array.from(new Set(visibleAccounts.map(a => a.account_id as string).filter(Boolean))),
    }
  }),

  setGatewayMap: (map) => set(state => {
    const entries = normalizeGatewayMap(isRecord(map) ? map : {})
    const selected = entries.find(entry => entry.gateway === state.connectedGateway) ?? entries[0]
    const nextGatewayMap = entries.reduce((acc, entry) => {
      entry.ids.forEach(id => { acc[id] = entry.gateway })
      return acc
    }, {} as Record<string, string>)
    const nextAccountIds = selected?.ids ?? []
    const nextGateway = selected?.gateway ?? null

    if (
      state.connectedGateway === nextGateway &&
      sameStringArray(state.accountIds, nextAccountIds) &&
      sameGatewayMap(state.gatewayMap, nextGatewayMap)
    ) {
      return state
    }

    return {
      connectedGateway: nextGateway,
      accountIds: nextAccountIds,
      gatewayMap: nextGatewayMap,
    }
  }),
}))
