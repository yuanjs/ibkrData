import { useEffect, useRef } from 'react'

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
    let active = true

    function connect() {
      if (!active) return
      const url = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}${path}?token=${TOKEN}`
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
  }, [path])

  const send = (data: unknown) => ws.current?.readyState === WebSocket.OPEN && ws.current.send(JSON.stringify(data))
  return { send }
}

