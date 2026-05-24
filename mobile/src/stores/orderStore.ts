import { create } from 'zustand'

interface OrderStore {
  orders: unknown[]
  setOrders: (orders: unknown[]) => void
  addUpdate: (update: unknown) => void
}

export const useOrderStore = create<OrderStore>(set => ({
  orders: [],
  setOrders: (orders) => set({ orders }),
  addUpdate: (update) => set(s => ({ orders: [update, ...s.orders.slice(0, 99)] })),
}))
