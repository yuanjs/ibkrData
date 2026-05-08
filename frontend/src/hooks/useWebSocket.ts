import { useEffect, useRef, useCallback } from 'react'

const TOKEN = import.meta.env.VITE_TOKEN || 'dev-token'

interface WebSocketOptions {
  onOpen?: () => void
  onClose?: () => void
}

export function useWebSocket(path: string, onMessage: (data: unknown) => void, options?: WebSocketOptions) {
  const ws = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<number | undefined>(undefined)
  const onMessageRef = useRef(onMessage)
  const optionsRef = useRef(options)
  onMessageRef.current = onMessage
  optionsRef.current = options

  useEffect(() => {
    function connect() {
      const url = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}${path}?token=${TOKEN}`
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
  }, [path])

  const send = (data: unknown) => ws.current?.readyState === WebSocket.OPEN && ws.current.send(JSON.stringify(data))
  return { send }
}
