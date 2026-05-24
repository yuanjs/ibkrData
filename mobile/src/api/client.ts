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
