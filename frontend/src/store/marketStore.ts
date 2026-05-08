import { create } from 'zustand'

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
  lastTick: Tick | null
  connected: boolean
  setConnected: (v: boolean) => void
  updateQuote: (q: Quote) => void
  updateTick: (t: Tick) => void
  initQuotes: (symbols: string[]) => void
}

export const useMarketStore = create<MarketStore>(set => ({
  quotes: {},
  lastTick: null,
  connected: false,
  setConnected: (connected) => set({ connected }),
  updateQuote: (q) => set(s => ({ 
    quotes: { 
      ...s.quotes, 
      [q.symbol]: { ...q, time: q.time || new Date().toISOString() } 
    } 
  })),
  updateTick: (t) => set({ lastTick: t }),
  initQuotes: (symbols) => set(s => {
    const newQuotes = { ...s.quotes }
    symbols.forEach(sym => {
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
    return { quotes: newQuotes }
  }),
}))
