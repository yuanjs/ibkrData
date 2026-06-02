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
    let active = true

    function connect() {
      if (!active) return
      const url = `${base}${path}?token=${token}`
      const socket = new WebSocket(url)
      ws.current = socket

      socket.onopen = () => {
        if (!active) return
        optionsRef.current?.onOpen?.()
      }
      socket.onmessage = (e) => {
        if (!active) return
        try { onMessageRef.current(JSON.parse(e.data)) } catch {}
      }
      socket.onclose = () => {
        if (!active) return
        optionsRef.current?.onClose?.()
        reconnectTimer.current = setTimeout(connect, 3000)
      }
    }
    connect()

    return () => {
      active = false
      clearTimeout(reconnectTimer.current)
      if (ws.current) {
        ws.current.onclose = null
        ws.current.close()
        ws.current = null
      }
    }
  }, [path, wsBaseUrl])

  const send = (data: unknown) => ws.current?.readyState === WebSocket.OPEN && ws.current.send(JSON.stringify(data))
  return { send }
}

