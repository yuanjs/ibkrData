import { create } from 'zustand'
import type { FuturesRollState, SymbolSubscription } from '../api/client'

export interface Quote {
  symbol: string
  bid: number | null
  ask: number | null
  last: number | null
  volume: number | null
  time: string
}

export interface Tick {
  symbol: string
  price: number
  size: number
  time: string
}

interface MarketStore {
  quotes: Record<string, Quote>
  subscriptions: Record<string, SymbolSubscription>
  futuresRollStates: Record<string, FuturesRollState>
  lastTick: Tick | null
  connected: boolean
  activeSymbol: string | null
  isFuturesSymbol: (sym: string | null | undefined) => boolean
  setActiveSymbol: (sym: string | null) => void
  setConnected: (v: boolean) => void
  updateQuote: (q: Quote) => void
  updateTick: (t: Tick) => void
  updateFuturesRollState: (state: FuturesRollState) => void
  initQuotes: (symbols: string[] | SymbolSubscription[]) => void
}

export const useMarketStore = create<MarketStore>((set, get) => ({
  quotes: {},
  subscriptions: {},
  futuresRollStates: {},
  lastTick: null,
  connected: false,
  activeSymbol: 'AUD.USD',
  isFuturesSymbol: (sym) => !!sym && get().subscriptions[sym]?.sec_type === 'FUT',
  setActiveSymbol: (activeSymbol) => set({ activeSymbol }),
  setConnected: (connected) => set({ connected }),
  updateQuote: (q) => set(s => ({
    quotes: {
      ...s.quotes,
      [q.symbol]: { ...q, time: q.time || new Date().toISOString() }
    }
  })),
  updateTick: (t) => set({ lastTick: t }),
  updateFuturesRollState: (state) => set(s => ({
    futuresRollStates: { ...s.futuresRollStates, [state.symbol]: state },
  })),
  initQuotes: (symbols) => set(s => {
    const newQuotes = { ...s.quotes }
    const subscriptions = { ...s.subscriptions }
    symbols.forEach(item => {
      const sym = typeof item === 'string' ? item : item.symbol
      if (typeof item !== 'string') subscriptions[sym] = item
      if (!newQuotes[sym]) {
        newQuotes[sym] = {
          symbol: sym,
          bid: null,
          ask: null,
          last: null,
          volume: null,
          time: new Date().toISOString()
        }
      }
    })
    return { quotes: newQuotes, subscriptions }
  }),
}))
