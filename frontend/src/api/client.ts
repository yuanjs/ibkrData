const BASE = '/api'
const TOKEN = import.meta.env.VITE_TOKEN || 'dev-token'

const headers = () => ({ Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' })

async function request<T = unknown>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, { ...options, headers: headers() })
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText)
    throw new Error(`API ${res.status}: ${detail}`)
  }
  return res.json() as Promise<T>
}

export const api = {
  get: <T = unknown>(path: string) => request<T>(`${BASE}${path}`),
  post: <T = unknown>(path: string, body: unknown) => request<T>(`${BASE}${path}`, { method: 'POST', body: JSON.stringify(body) }),
  put: <T = unknown>(path: string, body: unknown) => request<T>(`${BASE}${path}`, { method: 'PUT', body: JSON.stringify(body) }),
  del: <T = unknown>(path: string) => request<T>(`${BASE}${path}`, { method: 'DELETE' }),
}
