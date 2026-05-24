// Runtime-mutable server configuration (not baked into bundle)
// Defaults from .env, overridable in Settings page at runtime

let config = {
  apiUrl: process.env.EXPO_PUBLIC_API_URL || 'http://192.168.0.13:8002',
  wsUrl: process.env.EXPO_PUBLIC_WS_URL || 'ws://192.168.0.13:8002',
  token: process.env.EXPO_PUBLIC_API_TOKEN || 'dev-token',
}

export function getRuntimeConfig() {
  return { ...config }
}

export function setRuntimeConfig(c: Partial<typeof config>) {
  config = { ...config, ...c }
}
