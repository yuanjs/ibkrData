import { getRuntimeConfig } from '../config/runtimeConfig'

const headers = () => {
  const { token } = getRuntimeConfig()
  return { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
}

async function request<T = unknown>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, { ...options, headers: headers() })
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText)
    throw new Error(`API ${res.status}: ${detail}`)
  }
  return res.json() as Promise<T>
}

function query(params: Record<string, string | number | boolean | null | undefined>) {
  const qs = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') qs.set(key, String(value))
  }
  const text = qs.toString()
  return text ? `?${text}` : ''
}

export const api = {
  get: <T = unknown>(path: string) => {
    const { apiUrl } = getRuntimeConfig()
    return request<T>(`${apiUrl}/api${path}`)
  },
  post: <T = unknown>(path: string, body: unknown) => {
    const { apiUrl } = getRuntimeConfig()
    return request<T>(`${apiUrl}/api${path}`, { method: 'POST', body: JSON.stringify(body) })
  },
  put: <T = unknown>(path: string, body: unknown) => {
    const { apiUrl } = getRuntimeConfig()
    return request<T>(`${apiUrl}/api${path}`, { method: 'PUT', body: JSON.stringify(body) })
  },
  del: <T = unknown>(path: string) => {
    const { apiUrl } = getRuntimeConfig()
    return request<T>(`${apiUrl}/api${path}`, { method: 'DELETE' })
  },
}

export interface SymbolSubscription {
  symbol: string
  sec_type: string
  exchange: string
  currency: string
  active?: boolean
}

export interface FuturesActiveContract {
  symbol: string
  con_id: number
  contract_month: string | null
  local_symbol: string | null
  trading_class: string | null
  exchange: string | null
  currency: string | null
  multiplier: string | null
  effective_from?: string | null
  roll_event_id?: number | null
}

export interface FuturesRollState {
  symbol: string
  active: FuturesActiveContract
  previous?: FuturesActiveContract | null
  time?: string
}

export const futuresApi = {
  activeContract: (symbol: string, asOf?: string) =>
    api.get<FuturesActiveContract>(`/futures/${symbol}/active-contract${query({ as_of: asOf })}`),
  daily: (symbol: string, start: string, asOf?: string, adjustment = 'back_adjusted', includeLivePartial = false) =>
    api.get<any[]>(`/futures/${symbol}/daily${query({ start, as_of: asOf, adjustment, include_live_partial: includeLivePartial })}`),
  minute: (symbol: string, start: string, end: string, mode: 'active_raw' | 'adjusted' = 'active_raw', asOf?: string) =>
    api.get<any[]>(`/futures/${symbol}/minute${query({ start, end, mode, as_of: asOf })}`),
}
