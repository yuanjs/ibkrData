import { useEffect, useRef } from 'react'

interface WebSocketOptions {
  onOpen?: () => void
  onClose?: () => void
}

export function useWebSocket(
  path: string,
  onMessage: (data: unknown) => void,
  options?: WebSocketOptions,
  wsBaseUrl?: string,
) {
  const ws = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const onMessageRef = useRef(onMessage)
  const optionsRef = useRef(options)
  onMessageRef.current = onMessage
  optionsRef.current = options

  useEffect(() => {
    const token = process.env.EXPO_PUBLIC_API_TOKEN || 'dev-token'
    const base = wsBaseUrl || process.env.EXPO_PUBLIC_WS_URL || 'ws://localhost:8002'

    function connect() {
      const url = `${base}${path}?token=${token}`
      ws.current = new WebSocket(url)
      ws.current.onopen = () => {
        optionsRef.current?.onOpen?.()
      }
      ws.current.onmessage = (e) => {
        try { onMessageRef.current(JSON.parse(e.data)) } catch {}
      }
      ws.current.onclose = () => {
        optionsRef.current?.onClose?.()
        reconnectTimer.current = setTimeout(connect, 3000)
      }
    }
    connect()
    return () => {
      clearTimeout(reconnectTimer.current)
      ws.current?.close()
    }
  }, [path, wsBaseUrl])

  const send = (data: unknown) => ws.current?.readyState === WebSocket.OPEN && ws.current.send(JSON.stringify(data))
  return { send }
}
