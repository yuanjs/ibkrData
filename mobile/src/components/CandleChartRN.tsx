import { useRef, useEffect, useCallback } from 'react'
import { View, StyleSheet } from 'react-native'
import { WebView, type WebViewMessageEvent } from 'react-native-webview'
import { useTheme } from '../theme'
import { getProductConfig, getSymbolDecimalPlaces } from '../config/productConfig'

interface CandlestickData {
  time: number
  open: number
  high: number
  low: number
  close: number
}

interface TickData {
  symbol: string
  price: number
  size: number
  time: string
}

interface Props {
  symbol: string
  data: CandlestickData[]
  liveTick: TickData | null
  interval: string
  onIntervalChange: (inv: string) => void
}

export function CandleChartRN({ symbol, data, liveTick, interval, onIntervalChange }: Props) {
  const webViewRef = useRef<WebView>(null)
  const { colors, theme } = useTheme()
  const readyRef = useRef(false)

  // Keep refs to latest props for flushing on WebView ready
  const dataRef = useRef(data)
  const intervalRef = useRef(interval)
  const symbolRef = useRef(symbol)
  const liveTickRef = useRef(liveTick)
  const colorsRef = useRef(colors)
  dataRef.current = data
  intervalRef.current = interval
  symbolRef.current = symbol
  liveTickRef.current = liveTick
  colorsRef.current = colors

  const send = useCallback((msg: object) => {
    if (webViewRef.current && readyRef.current) {
      webViewRef.current.injectJavaScript(
        `window.__handleMsg(${JSON.stringify(JSON.stringify(msg))});true;`
      )
    }
  }, [])

  const buildThemeMsg = useCallback(() => ({
    type: 'theme' as const,
    colors: {
      bg: colors.background,
      text: colors.textPrimary,
      grid: colors.border,
      up: '#26a641',
      down: '#d32f2f',
      ma3: '#2962ff',
      ma5: '#ff9800',
      ma10: '#000000',
      kLine: '#2962ff',
      dLine: '#ff9800',
      jLine: '#9c27b0',
      tooltipBg: colors.tooltip,
      raised: colors.raised,
      secondary: colors.textSecondary,
    },
  }), [colors])

  const flushCurrentState = useCallback(() => {
    if (!webViewRef.current) return
    const c = colorsRef.current
    send({
      type: 'theme',
      colors: {
        bg: c.background,
        text: c.textPrimary,
        grid: c.border,
        up: '#26a641',
        down: '#d32f2f',
        ma3: '#2962ff',
        ma5: '#ff9800',
        ma10: '#000000',
        kLine: '#2962ff',
        dLine: '#ff9800',
        jLine: '#9c27b0',
        tooltipBg: c.tooltip,
        raised: c.raised,
        secondary: c.textSecondary,
      },
    })
    var cfg = getProductConfig(symbolRef.current)
    var dp = getSymbolDecimalPlaces(symbolRef.current)
    send({
      type: 'candles',
      data: dataRef.current,
      interval: intervalRef.current,
      symbol: symbolRef.current,
      timezone: cfg.timezone,
      rollHour: cfg.rollHour,
      rollMinute: cfg.rollMinute,
      decimalPlaces: dp,
      theme: {
        bg: c.background,
        text: c.textPrimary,
        grid: c.border,
        up: '#26a641',
        down: '#d32f2f',
        ma3: '#2962ff',
        ma5: '#ff9800',
        ma10: '#000000',
        kLine: '#2962ff',
        dLine: '#ff9800',
        jLine: '#9c27b0',
        tooltipBg: c.tooltip,
        raised: c.raised,
        secondary: c.textSecondary,
      },
    })
    const lt = liveTickRef.current
    if (lt && dataRef.current.length > 0) {
      send({ type: 'tick', tick: lt, interval: intervalRef.current })
    }
  }, [send])

  // Send candles data when symbol/interval/data changes
  useEffect(() => {
    var cfg = getProductConfig(symbol)
    const decPlaces = getSymbolDecimalPlaces(symbol)
    send({
      type: 'candles',
      data,
      interval,
      symbol,
      timezone: cfg.timezone,
      rollHour: cfg.rollHour,
      rollMinute: cfg.rollMinute,
      decimalPlaces: decPlaces,
      theme: {
        bg: colors.background,
        text: colors.textPrimary,
        grid: colors.border,
        up: '#26a641',
        down: '#d32f2f',
        ma3: '#2962ff',
        ma5: '#ff9800',
        ma10: '#000000',
        kLine: '#2962ff',
        dLine: '#ff9800',
        jLine: '#9c27b0',
        tooltipBg: colors.tooltip,
        raised: colors.raised,
        secondary: colors.textSecondary,
      },
    })
  }, [data, interval, symbol, colors, send, getProductConfig])

  // Send tick updates — include product config for daily bar rollhour logic
  useEffect(() => {
    if (!liveTick || data.length === 0) return
    var cfg = getProductConfig(symbol)
    send({ type: 'tick', tick: liveTick, interval, timezone: cfg.timezone, rollHour: cfg.rollHour, rollMinute: cfg.rollMinute })
  }, [liveTick, interval, data.length, send, symbol])

  // Send theme updates
  useEffect(() => {
    send(buildThemeMsg())
  }, [theme, colors, send, buildThemeMsg])

  const handleMessage = useCallback((event: WebViewMessageEvent) => {
    try {
      const msg = JSON.parse(event.nativeEvent.data)
      if (msg.type === 'ready') {
        readyRef.current = true
        flushCurrentState()
      } else if (msg.type === 'intervalChange') {
        onIntervalChange(msg.interval)
      } else if (msg.type === 'log') {
        console.log('[WebView]', msg.text)
      }
    } catch {}
  }, [onIntervalChange, flushCurrentState])

  return (
    <View style={[styles.container, { backgroundColor: colors.background }]}>
      <WebView
        ref={webViewRef}
        source={require('../../assets/chart.html')}
        style={styles.webview}
        onMessage={handleMessage}
        javaScriptEnabled={true}
        domStorageEnabled={true}
        scrollEnabled={false}
        bounces={false}
        overScrollMode="never"
        originWhitelist={['*']}
      />
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    minHeight: 300,
  },
  webview: {
    flex: 1,
    backgroundColor: 'transparent',
  },
})
