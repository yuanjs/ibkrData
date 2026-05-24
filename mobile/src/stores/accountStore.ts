import { create } from 'zustand'

interface AccountStore {
  summary: Record<string, unknown>
  positions: unknown[]
  setAccount: (data: { accounts: unknown[]; positions: unknown[] }) => void
}

export const useAccountStore = create<AccountStore>(set => ({
  summary: {},
  positions: [],
  setAccount: ({ accounts, positions }) => set({ summary: (accounts[0] as Record<string, unknown>) ?? {}, positions }),
}))
