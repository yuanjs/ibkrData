import { useEffect, useRef, useCallback, useState } from 'react'
import { createChart, CandlestickSeries, LineSeries, type IChartApi, type ISeriesApi, type CandlestickData } from 'lightweight-charts'
import { getProductConfig, getSymbolDecimalPlaces } from '../config/productConfig'

interface Props {
  symbol?: string
  data: CandlestickData[]
  liveTick?: any
  interval: string
  onIntervalChange: (v: string) => void
}

// Helper to calculate KDJ matching candlestack.js logic
function calculateKDJData(candles: any[]) {
  const kdjPeriod = 5
  const kdjSlowK = 3
  const kdjSlowD = 3

  const kData: any[] = []
  const dData: any[] = []
  const jData: any[] = []

  if (candles.length < kdjPeriod + kdjSlowK) {
    return { k: kData, d: dData, j: jData }
  }

  // First pass: Calculate PeriodHigh and PeriodLow
  const processed = candles.map((c, i) => {
    const start = Math.max(0, i - kdjPeriod + 1)
    const periodSlice = candles.slice(start, i + 1)
    let maxH = -Infinity
    let minL = Infinity
    for (const p of periodSlice) {
      maxH = Math.max(maxH, p.high)
      minL = Math.min(minL, p.low)
    }
    return { ...c, PeriodHigh: maxH, PeriodLow: minL }
  })

  // Second pass: Calculate K, D, J
  for (let i = 0; i < processed.length; i++) {
    if (i >= kdjPeriod + kdjSlowK - 1) {
      let sumLow = 0
      let sumHigh = 0
      for (let j = i - kdjSlowK + 1; j <= i; j++) {
        sumLow += processed[j].close - processed[j].PeriodLow
        sumHigh += processed[j].PeriodHigh - processed[j].PeriodLow
      }
      const k = sumHigh === 0 ? 50 : (sumLow / sumHigh) * 100
      processed[i].K = k
      kData.push({ time: processed[i].time, value: k })

      if (i >= kdjPeriod + kdjSlowK + kdjSlowD - 2) {
        let sumK = 0
        let countK = 0
        for (let j = i - kdjSlowD + 1; j <= i; j++) {
          if (processed[j].K !== undefined) {
            sumK += processed[j].K
            countK++
          }
        }
        const d = countK > 0 ? sumK / countK : 50
        processed[i].D = d
        dData.push({ time: processed[i].time, value: d })

        const jVal = 3 * d - 2 * k
        jData.push({ time: processed[i].time, value: jVal })
      }
    }
  }
  return { k: kData, d: dData, j: jData }
}

// Helper to get seconds from interval string
function getIntervalSeconds(inv: string) {
  if (inv === '1d') return 86400
  if (inv === '1w') return 7 * 86400
  if (inv.endsWith('min')) return parseInt(inv) * 60
  if (inv.endsWith('m')) return parseInt(inv) * 60
  if (inv.endsWith('h')) return parseInt(inv) * 3600
  if (inv.endsWith('s')) return parseInt(inv)
  return 60
}

function getEffectiveBucketTime(tickTimeSec: number, sym?: string): number {
  const config = sym ? getProductConfig(sym) : undefined
  if (!config) return Math.floor(tickTimeSec / 86400) * 86400 + 43200

  // Get local time in the product's exchange timezone
  const d = new Date(tickTimeSec * 1000)
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: config.timezone,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  }).formatToParts(d)

  const get = (t: string) => Number(parts.find(p => p.type === t)?.value ?? 0)
  const hour = get('hour'), minute = get('minute')

  // Determine effective date: if after roll time, belongs to next trading day
  let [y, m, day] = [get('year'), get('month'), get('day')]
  if (hour > config.rollHour || (hour === config.rollHour && minute >= config.rollMinute)) {
    // Add one day
    const next = new Date(Date.UTC(y, m - 1, day + 1))
    y = next.getUTCFullYear()
    m = next.getUTCMonth() + 1
    day = next.getUTCDate()
  }

  return Math.floor(Date.UTC(y, m - 1, day, 12) / 1000)
}

/** Get Unix timestamp for midnight (start of the day) for a given time in the product's exchange timezone */
function getMidnightSec(timeSec: number, symbol?: string): number {
  const config = symbol ? getProductConfig(symbol) : undefined
  const tz = config?.timezone || 'America/New_York'
  const d = new Date(timeSec * 1000)
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  }).formatToParts(d)

  const get = (t: string) => {
    const v = parts.find(p => p.type === t)?.value ?? '0'
    return parseInt(v, 10)
  }

  // Some locales or environments might return 24 for midnight; normalize to 0.
  const hour = get('hour') % 24
  const minute = get('minute')
  const second = get('second')

  const elapsedSec = hour * 3600 + minute * 60 + second
  return timeSec - elapsedSec
}

export function CandleChart({ symbol, data, liveTick, interval, onIntervalChange }: Props) {
  const mainContainerRef = useRef<HTMLDivElement>(null)
  const kdjContainerRef = useRef<HTMLDivElement>(null)
  const tooltipRef = useRef<HTMLDivElement>(null)
  const mobileInfoRef = useRef<HTMLDivElement>(null)
  const isMobileRef = useRef(window.innerWidth < 768)

  // Responsive heights
  const isMobile = window.innerWidth < 768
  const availableHeight = isMobile ? window.innerHeight - 88 : 440
  const kdjHeight = isMobile ? Math.floor(availableHeight * 0.25) : 120
  const mainHeight = isMobile ? availableHeight - kdjHeight : 320

  const chartRef = useRef<IChartApi | undefined>(undefined)
  const seriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const ma3SeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const ma5SeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const ma10SeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)

  const kdjChartRef = useRef<IChartApi | undefined>(undefined)
  const kSeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const dSeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const jSeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)

  const kdjDataRef = useRef<{ k: any[]; d: any[]; j: any[] }>({ k: [], d: [], j: [] })
  const lastDataRef = useRef<any[]>([])

  // Track whether the user has scrolled away from the latest data
  const [showGoToLatest, setShowGoToLatest] = useState(false)
  // Ref to suppress false detections during programmatic scroll/fit operations
  const programmaticScrollRef = useRef(false)

  /** Read a CSS custom property from :root */
  const cssVar = useCallback((name: string): string => {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || ''
  }, [])

  const isLineChart = interval === '1s' || interval === '5s' || interval === '10s'
  const decPlaces = getSymbolDecimalPlaces(symbol)

  const getTimezone = useCallback(() => {
    return getProductConfig(symbol || '').timezone
  }, [symbol])

  // Format time for display
  const formatTime = useCallback((timeSec: number) => {
    const date = new Date(timeSec * 1000)
    if (isNaN(date.getTime())) return String(timeSec)
    const tz = getTimezone()
    if (interval === '1d') {
      return date.toLocaleString('en-GB', {
        timeZone: tz,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour12: false,
      }).replace(',', '')
    }
    return date.toLocaleString('en-GB', {
      timeZone: tz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).replace(',', '')
  }, [getTimezone, interval])

  // Track mobile/desktop for tooltip layout
  useEffect(() => {
    const handleResize = () => { isMobileRef.current = window.innerWidth < 768 }
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  // Create / destroy charts
  useEffect(() => {
    if (!mainContainerRef.current) return

    let rafId = 0

    const tz = getTimezone()

    // Read theme colors from CSS variables
    const bgColor = cssVar('--bg-elevated') || '#0f1117'
    const textColor = cssVar('--text-secondary') || '#9ca3af'
    const gridColor = cssVar('--border') || '#1f2937'

    // Create main chart
    const chart = createChart(mainContainerRef.current, {
      layout: { background: { color: bgColor }, textColor },
      grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
      crosshair: {
        vertLine: { color: '#9ca3af', labelBackgroundColor: '#9ca3af', width: 2, style: 0 },
        horzLine: { visible: false, labelVisible: false },
      },
      width: mainContainerRef.current.clientWidth,
      height: mainHeight,
      // 左键拖动 = 平移；滚轮 = 缩放
      handleScroll: {
        mouseWheel: false,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      },
      handleScale: {
        mouseWheel: true,
        pinch: true,
        axisPressedMouseMove: false,
        axisDoubleClickReset: true,
      },
      rightPriceScale: {
        minimumWidth: isMobile ? 80 : 120,
      },
      timeScale: {
        timeVisible: interval !== '1d',
        secondsVisible: false,
        tickMarkFormatter: (time: any) => {
          const date = new Date(time * 1000)
          if (isNaN(date.getTime())) return ''
          if (interval === '1d') {
            return date.toLocaleDateString('en-GB', {
              timeZone: tz,
              day: '2-digit',
              month: '2-digit',
            })
          }
          return date.toLocaleTimeString('en-GB', {
            timeZone: tz,
            hour: '2-digit',
            minute: '2-digit',
            hour12: false,
          })
        },
      },
      localization: {
        timeFormatter: (time: any) => {
          const date = new Date(time * 1000)
          if (isNaN(date.getTime())) return String(time)
          if (interval === '1d') {
            return date.toLocaleString('en-GB', {
              timeZone: tz,
              year: 'numeric',
              month: '2-digit',
              day: '2-digit',
              hour12: false,
            }).replace(',', '')
          }
          return date.toLocaleString('en-GB', {
            timeZone: tz,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
          }).replace(',', '')
        },
      },
    })
    chartRef.current = chart

    // Create main series
    let series: ISeriesApi<any>
    if (isLineChart) {
      series = chart.addSeries(LineSeries, {
        color: '#3b82f6',
        lineWidth: 2,
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 5,
        crosshairMarkerBorderColor: '#fff',
        crosshairMarkerBackgroundColor: '#3b82f6',
        priceFormat: { type: 'price', precision: decPlaces, minMove: Math.pow(10, -decPlaces) },
      })
    } else {
      series = chart.addSeries(CandlestickSeries, {
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderUpColor: '#22c55e',
        borderDownColor: '#ef4444',
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444',
        priceFormat: { type: 'price', precision: decPlaces, minMove: Math.pow(10, -decPlaces) },
      })
    }
    seriesRef.current = series

    // MA series (only for candle charts)
    if (!isLineChart) {
      ma3SeriesRef.current = chart.addSeries(LineSeries, {
        color: '#3b82f6',
        lineWidth: 2,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      })
      ma5SeriesRef.current = chart.addSeries(LineSeries, {
        color: '#eab308',
        lineWidth: 2,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      })
      ma10SeriesRef.current = chart.addSeries(LineSeries, {
        color: '#a855f7',
        lineWidth: 3,
        lineStyle: 2,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      })
    } else {
      ma3SeriesRef.current = undefined
      ma5SeriesRef.current = undefined
      ma10SeriesRef.current = undefined
    }

    // KDJ chart (only for candle charts)
    let kdjChart: IChartApi | undefined
    if (!isLineChart && kdjContainerRef.current) {
      kdjChart = createChart(kdjContainerRef.current, {
        layout: { background: { color: bgColor }, textColor },
        grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
        width: mainContainerRef.current.clientWidth,
        height: kdjHeight,
        handleScroll: {
          mouseWheel: false,
          pressedMouseMove: false,
          horzTouchDrag: false,
          vertTouchDrag: false,
        },
        handleScale: {
          mouseWheel: false,
          pinch: false,
          axisPressedMouseMove: false,
          axisDoubleClickReset: false,
        },
        rightPriceScale: {
          minimumWidth: isMobile ? 80 : 120,
          autoScale: false,
        },
        timeScale: {
          visible: false,
        },
        crosshair: {
          vertLine: { visible: true, color: '#9ca3af', labelBackgroundColor: '#9ca3af', width: 2, style: 0 },
          horzLine: { visible: false, labelVisible: false },
        },
      })
      kdjChartRef.current = kdjChart

      // Fix KDJ Y-axis range to -20 ~ 120 so 0/100 are symmetric, applies to both mobile and desktop
      const fixedRange = { minValue: -20, maxValue: 120 }
      const fixedAutoscale = () => ({ priceRange: fixedRange })

      kSeriesRef.current = kdjChart.addSeries(LineSeries, {
        color: '#3b82f6', lineWidth: 1, title: '',
        lastValueVisible: true, priceLineVisible: false, crosshairMarkerVisible: false,
        autoscaleInfoProvider: fixedAutoscale,
      })
      dSeriesRef.current = kdjChart.addSeries(LineSeries, {
        color: '#eab308', lineWidth: 1, title: '',
        lastValueVisible: true, priceLineVisible: false, crosshairMarkerVisible: false,
        autoscaleInfoProvider: fixedAutoscale,
      })
      jSeriesRef.current = kdjChart.addSeries(LineSeries, {
        color: '#a855f7', lineWidth: 2, title: '',
        lastValueVisible: true, priceLineVisible: false, crosshairMarkerVisible: false,
        autoscaleInfoProvider: fixedAutoscale,
      })

      // Add 0 and 100 reference lines
      kSeriesRef.current.createPriceLine({ price: 0, color: '#4b5563', lineWidth: 1, lineStyle: 0, axisLabelVisible: true, title: '' })
      kSeriesRef.current.createPriceLine({ price: 100, color: '#4b5563', lineWidth: 1, lineStyle: 0, axisLabelVisible: true, title: '' })

      // Continuous polling sync: read main chart's logical range each frame
      // and apply to KDJ with bar-index offset. Does NOT depend on LWTC
      // events, which may not fire reliably during drag-to-pan gestures.
      let lastFrom = -1
      let lastTo = -1
      let cachedOffset = -1
      let lastKdjK0Time = -1

      const syncLoop = () => {
        if (!chartRef.current || !kdjChartRef.current) return
        const mr = chartRef.current.timeScale().getVisibleLogicalRange()
        if (!mr) return
        const kdjK = kdjDataRef.current.k
        if (kdjK.length === 0) {
          rafId = requestAnimationFrame(syncLoop)
          return
        }

        // Cache offset calculation: only re-calculate if KDJ first point time or data changes
        const currentKdjK0Time = kdjK[0].time
        if (cachedOffset === -1 || currentKdjK0Time !== lastKdjK0Time) {
          const mainData = lastDataRef.current
          if (mainData.length > 0) {
            const idx = mainData.findIndex(x => x.time === currentKdjK0Time)
            if (idx !== -1) {
              cachedOffset = idx
              lastKdjK0Time = currentKdjK0Time
            }
          }
        }

        if (cachedOffset !== -1) {
          const from = mr.from - cachedOffset
          const to = mr.to - cachedOffset
          if (from !== lastFrom || to !== lastTo) {
            lastFrom = from
            lastTo = to
            try { kdjChartRef.current.timeScale().setVisibleLogicalRange({ from, to }) } catch { }
          }
        }
        rafId = requestAnimationFrame(syncLoop)
      }
      rafId = requestAnimationFrame(syncLoop)

      // Sync crosshair vertical line from main chart to KDJ chart
      chart.subscribeCrosshairMove((param) => {
        const kdjChart = kdjChartRef.current
        const kSeries = kSeriesRef.current
        if (!kdjChart || !kSeries) return
        if (param.time) {
          // Look up K value at this time from our cached data
          const timeSec = typeof param.time === 'number' ? param.time : Math.floor(new Date(param.time as string).getTime() / 1000)
          const kPoint = kdjDataRef.current.k.find(x => x.time === timeSec)
          const price = kPoint?.value ?? 50
          try { kdjChart.setCrosshairPosition(price, param.time, kSeries) } catch { }
        } else {
          try { kdjChart.clearCrosshairPosition() } catch { }
        }
      })
    } else {
      kdjChartRef.current = undefined
      kSeriesRef.current = undefined
      dSeriesRef.current = undefined
      jSeriesRef.current = undefined
    }

    // Track whether user has scrolled away from the right edge (latest data)
    // Only sets showGoToLatest to TRUE when user scrolls away.
    // Never sets it to FALSE — that only happens via button click or data reload.
    chart.timeScale().subscribeVisibleLogicalRangeChange((logicalRange) => {
      // Skip detection during programmatic scroll/fit operations
      if (programmaticScrollRef.current) return
      if (!logicalRange || !seriesRef.current) return
      const dataLength = lastDataRef.current.length
      if (dataLength === 0) return
      // Show button when the right edge is more than 3 bars away from the latest
      const rightEdge = logicalRange.to
      if (rightEdge < dataLength - 3) {
        setShowGoToLatest(true)
      }
    })

    // Tooltip on crosshair move
    chart.subscribeCrosshairMove((param) => {
      const tt = tooltipRef.current
      const mi = mobileInfoRef.current
      if (!mainContainerRef.current) return

      const isMobile = isMobileRef.current

      // Hide both by default; the active one will be shown below
      if (tt) tt.style.display = 'none'
      if (mi) mi.style.display = 'none'

      if (
        param.point === undefined ||
        !param.time ||
        param.point.x < 0 ||
        param.point.x > mainContainerRef.current.clientWidth ||
        param.point.y < 0 ||
        param.point.y > mainHeight
      ) {
        return
      }

      const sData = param.seriesData.get(series) as any
      if (!sData) return

      const timeSec = typeof sData.time === 'number' ? sData.time : Math.floor(Date.now() / 1000)
      const timeStr = formatTime(timeSec)

      // Lookup MA values safely
      const ma3Val = ma3SeriesRef.current ? (param.seriesData.get(ma3SeriesRef.current) as any)?.value : undefined
      const ma5Val = ma5SeriesRef.current ? (param.seriesData.get(ma5SeriesRef.current) as any)?.value : undefined
      const ma10Val = ma10SeriesRef.current ? (param.seriesData.get(ma10SeriesRef.current) as any)?.value : undefined

      // Lookup KDJ values
      const kVal = kdjDataRef.current.k.find((x) => x.time === timeSec)?.value
      const dVal = kdjDataRef.current.d.find((x) => x.time === timeSec)?.value
      const jVal = kdjDataRef.current.j.find((x) => x.time === timeSec)?.value

      const tp = 'color:var(--text-primary)'
      const ts = 'color:var(--text-secondary)'

      if (isMobile && mi) {
        // Mobile: fixed info panel at top-left corner
        mi.style.display = 'block'
        if (isLineChart) {
          mi.innerHTML = `<span style="${tp};font-size:0.85rem">${timeStr}</span> <span class="text-blue-400" style="font-family:monospace;font-size:0.85rem">${(sData.value ?? sData.close)?.toFixed(decPlaces) ?? '-'}</span>`
        } else {
          const cO = sData.open?.toFixed(decPlaces)
          const cH = sData.high?.toFixed(decPlaces)
          const cL = sData.low?.toFixed(decPlaces)
          const cC = sData.close?.toFixed(decPlaces)
          const oCls = sData.open > sData.close ? 'text-red-400' : 'text-green-400'
          const hCls = sData.high > sData.close ? 'text-red-400' : 'text-green-400'
          const lCls = sData.low > sData.close ? 'text-red-400' : 'text-green-400'
          const cCls = sData.close >= sData.open ? 'text-green-400' : 'text-red-400'
          mi.innerHTML = `
            <div style="${tp};font-size:0.85rem;margin-bottom:0.125rem">${timeStr}</div>
            <div style="display:flex;flex-wrap:wrap;gap:2px 8px;font-size:0.8rem;font-family:monospace">
              <span><span style="${ts}">O</span><span class="${oCls}">${cO}</span></span>
              <span><span style="${ts}">H</span><span class="${hCls}">${cH}</span></span>
              <span><span style="${ts}">L</span><span class="${lCls}">${cL}</span></span>
              <span><span style="${ts}">C</span><span class="${cCls}">${cC}</span></span>
              <span><span style="${ts}">3M</span><span class="text-blue-400">${ma3Val?.toFixed(decPlaces) ?? '-'}</span></span>
              <span><span style="${ts}">5M</span><span style="color:#eab308">${ma5Val?.toFixed(decPlaces) ?? '-'}</span></span>
              <span><span style="${ts}">10M</span><span class="text-purple-400">${ma10Val?.toFixed(decPlaces) ?? '-'}</span></span>
              <span style="border-left:1px solid var(--border);padding-left:3px"><span style="${ts}">K</span><span class="text-blue-400">${kVal?.toFixed(2) ?? '-'}</span></span>
              <span><span style="${ts}">D</span><span style="color:#eab308">${dVal?.toFixed(2) ?? '-'}</span></span>
              <span><span style="${ts}">J</span><span class="text-purple-400">${jVal?.toFixed(2) ?? '-'}</span></span>
            </div>
          `
        }
        return
      }

      // Desktop: floating tooltip following crosshair
      if (!tt) return
      tt.style.display = 'block'
      if (isLineChart) {
        tt.innerHTML = `
          <div style="font-weight:bold;${tp};font-size:0.875rem;white-space:nowrap">${timeStr}</div>
          <div style="margin-top:0.25rem;font-size:0.75rem"><span style="${ts}">Price:</span><span class="text-blue-400" style="margin-left:0.5rem;font-family:monospace">${(sData.value ?? sData.close)?.toFixed(decPlaces) ?? '-'}</span></div>
        `
      } else {
        tt.innerHTML = `
          <div style="font-weight:bold;${tp};font-size:0.875rem;white-space:nowrap">${timeStr}</div>
          <div class="grid grid-cols-2 gap-x-3 gap-y-1 text-xs mt-2">
            <div class="flex justify-between w-16"><span style="${ts}">O:</span><span class="${sData.open > sData.close ? 'text-red-400' : 'text-green-400'}">${sData.open?.toFixed(decPlaces)}</span></div>
            <div class="flex justify-between w-16"><span style="${ts}">H:</span><span class="${sData.high > sData.close ? 'text-red-400' : 'text-green-400'}">${sData.high?.toFixed(decPlaces)}</span></div>
            <div class="flex justify-between w-16"><span style="${ts}">L:</span><span class="${sData.low > sData.close ? 'text-red-400' : 'text-green-400'}">${sData.low?.toFixed(decPlaces)}</span></div>
            <div class="flex justify-between w-16"><span style="${ts}">C:</span><span class="${sData.close >= sData.open ? 'text-green-400' : 'text-red-400'}">${sData.close?.toFixed(decPlaces)}</span></div>
          </div>
          <div class="flex gap-4 mt-2 text-[10px] font-mono">
            <div class="flex items-center gap-1"><span style="${ts}">3M:</span><span class="text-blue-400">${ma3Val?.toFixed(decPlaces) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><span style="${ts}">5M:</span><span style="color:#eab308">${ma5Val?.toFixed(decPlaces) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><span style="${ts}">10M:</span><span class="text-purple-400">${ma10Val?.toFixed(decPlaces) ?? '-'}</span></div>
          </div>
          <div class="flex gap-3 mt-1 text-[10px] font-mono pt-1" style="border-top:1px solid var(--border)">
            <div class="flex items-center gap-1"><span style="${ts}">K:</span><span class="text-blue-400">${kVal?.toFixed(2) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><span style="${ts}">D:</span><span style="color:#eab308">${dVal?.toFixed(2) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><span style="${ts}">J:</span><span class="text-purple-400">${jVal?.toFixed(2) ?? '-'}</span></div>
          </div>
        `
      }

      // Positioning logic to keep tooltip within view
      const ttWidth = tt.offsetWidth
      const ttHeight = tt.offsetHeight
      const x = Math.min(Math.max(0, param.point.x + 15), mainContainerRef.current.clientWidth - ttWidth - 5)
      const y = Math.min(Math.max(10, param.point.y - ttHeight / 2), mainHeight - ttHeight - 5)
      tt.style.left = `${x}px`
      tt.style.top = `${y}px`
    })

    const resizeObserver = new ResizeObserver((entries) => {
      if (entries.length === 0 || !entries[0].contentRect) return
      const { width } = entries[0].contentRect
      if (chartRef.current) {
        chartRef.current.applyOptions({ width })
      }
      if (kdjChartRef.current) {
        kdjChartRef.current.applyOptions({ width })
      }
    })
    resizeObserver.observe(mainContainerRef.current)

    // Watch for theme changes and update chart colors
    const themeObserver = new MutationObserver(() => {
      const newBg = getComputedStyle(document.documentElement).getPropertyValue('--bg-elevated').trim() || '#0f1117'
      const newText = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#9ca3af'
      const newGrid = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#1f2937'
      const opts = {
        layout: { background: { color: newBg }, textColor: newText },
        grid: { vertLines: { color: newGrid }, horzLines: { color: newGrid } },
      }
      chart.applyOptions(opts)
      if (kdjChart) kdjChart.applyOptions(opts)
    })
    themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })

    // Clear data refs on chart recreation to prevent stale sync during interval switch
    lastDataRef.current = []
    kdjDataRef.current = { k: [], d: [], j: [] }

    return () => {
      cancelAnimationFrame(rafId)
      themeObserver.disconnect()
      resizeObserver.disconnect()
      if (tooltipRef.current) tooltipRef.current.style.display = 'none'
      if (mobileInfoRef.current) mobileInfoRef.current.style.display = 'none'
      chart.remove()
      chartRef.current = undefined
      seriesRef.current = undefined
      ma3SeriesRef.current = undefined
      ma5SeriesRef.current = undefined
      ma10SeriesRef.current = undefined
      if (kdjChart) {
        kdjChart.remove()
      }
      kdjChartRef.current = undefined
      kSeriesRef.current = undefined
      dSeriesRef.current = undefined
      jSeriesRef.current = undefined
    }
  }, [interval, symbol, isLineChart, getTimezone, formatTime, cssVar])

  // Set data when data changes
  useEffect(() => {
    if (!data.length || !seriesRef.current) return

    // Normalize time to numeric seconds
    const normalizedData = data.map((d) => ({
      ...d,
      time: typeof d.time === 'string' ? Math.floor(new Date(d.time as string).getTime() / 1000) : d.time,
    }))
    lastDataRef.current = normalizedData.map((d) => ({ ...d }))

    if (isLineChart) {
      seriesRef.current.setData(normalizedData.map((d) => ({ time: d.time, value: (d as any).close })))
    } else {
      seriesRef.current.setData(normalizedData as any)

      // Calculate and set MA data
      const ma3Data: any[] = []
      const ma5Data: any[] = []
      const ma10Data: any[] = []
      for (let i = 0; i < normalizedData.length; i++) {
        if (i >= 2) {
          let sum3 = 0
          for (let j = 0; j < 3; j++) sum3 += (normalizedData[i - j] as any).close
          ma3Data.push({ time: normalizedData[i].time, value: sum3 / 3 })
        }
        if (i >= 4) {
          let sum5 = 0
          for (let j = 0; j < 5; j++) sum5 += (normalizedData[i - j] as any).close
          ma5Data.push({ time: normalizedData[i].time, value: sum5 / 5 })
        }
        if (i >= 9) {
          let sum10 = 0
          for (let j = 0; j < 10; j++) sum10 += (normalizedData[i - j] as any).close
          ma10Data.push({ time: normalizedData[i].time, value: sum10 / 10 })
        }
      }
      ma3SeriesRef.current?.setData(ma3Data)
      ma5SeriesRef.current?.setData(ma5Data)
      ma10SeriesRef.current?.setData(ma10Data)

      // Calculate KDJ
      if (kSeriesRef.current && dSeriesRef.current && jSeriesRef.current) {
        const kdj = calculateKDJData(normalizedData)
        kdjDataRef.current = kdj
        kSeriesRef.current.setData(kdj.k)
        dSeriesRef.current.setData(kdj.d)
        jSeriesRef.current.setData(kdj.j)
      }
    }

    // Set visible range for both Line and Candle charts
    if (chartRef.current) {
      programmaticScrollRef.current = true

      // Fit KDJ chart if applicable
      if (!isLineChart && kdjChartRef.current && kdjDataRef.current.k.length > 0) {
        kdjChartRef.current.timeScale().fitContent()
      }

      // Determine if the interval is intraday (seconds, minutes, or hours)
      const isIntraday = interval.endsWith('s') || interval.endsWith('m') || interval.endsWith('h') || interval.endsWith('min')

      if (isIntraday) {
        // Intraday: default to showing only today's data if available
        const lastPoint = normalizedData[normalizedData.length - 1]
        const midnightSec = getMidnightSec(lastPoint.time as number, symbol)
        const firstTodayIdx = normalizedData.findIndex(d => (d.time as number) >= midnightSec)
        if (firstTodayIdx > 0) {
          chartRef.current.timeScale().setVisibleLogicalRange({
            from: firstTodayIdx,
            to: normalizedData.length - 1,
          })
        } else {
          chartRef.current.timeScale().fitContent()
        }
      } else {
        // Daily or weekly: show all loaded data
        chartRef.current.timeScale().fitContent()
      }
    }
    // Reset the "Go to Latest" button state
    setShowGoToLatest(false)
    // Allow detection again after a short delay (let the range change event settle)
    setTimeout(() => { programmaticScrollRef.current = false }, 300)
  }, [data, isLineChart, interval, symbol])

  // Process live ticks (skip for weekly — no meaningful real-time bucketing)
  useEffect(() => {
    if (!liveTick?.price || lastDataRef.current.length === 0 || !seriesRef.current || interval === '1w') return

    const currentData = lastDataRef.current
    const lastCandle = currentData[currentData.length - 1] as any
    const newClose = liveTick.price

    const tickTimeSec = liveTick.time
      ? Math.floor(new Date(liveTick.time).getTime() / 1000)
      : Math.floor(Date.now() / 1000)

    let currentBucketTime: number
    if (interval === '1d') {
      currentBucketTime = getEffectiveBucketTime(tickTimeSec, symbol)
    } else {
      const invSec = getIntervalSeconds(interval)
      currentBucketTime = tickTimeSec - (tickTimeSec % invSec)
    }

    // Comparison logic for new candle
    const isNewCandle = interval === '1d'
      ? (currentBucketTime !== lastCandle.time)
      : (Number(currentBucketTime) > Number(lastCandle.time))

    if (isNewCandle) {
      if (Number(currentBucketTime) > Number(lastCandle.time)) {
        // Tick belongs to a new trading day (e.g., after roll hour)
        const newCandle = { time: currentBucketTime, open: newClose, high: newClose, low: newClose, close: newClose }
        if (isLineChart) {
          seriesRef.current.update({ time: currentBucketTime as any, value: newClose })
        } else {
          seriesRef.current.update(newCandle as any)
        }
        currentData.push(newCandle)
      } else {
        // currentBucketTime < lastCandle.time: tick belongs to a past candle
        // (e.g., last candle is tomorrow's rolled bar, tick is today's pre-roll data).
        // Update internal data silently — chart already has correct OHLC from DB.
        const pastCandle = currentData.find(d => d.time === currentBucketTime)
        if (pastCandle) {
          pastCandle.high = Math.max(pastCandle.high, newClose)
          pastCandle.low = Math.min(pastCandle.low, newClose)
          pastCandle.close = newClose
        }
        // Don't call chart update — past candle data is already finalized in DB
      }
    } else {
      const updateTime = interval === '1d' ? lastCandle.time : Math.max(Number(lastCandle.time), Number(currentBucketTime))
      if (isLineChart) {
        seriesRef.current.update({ time: updateTime, value: newClose })
      } else {
        seriesRef.current.update({
          time: updateTime,
          open: lastCandle.open,
          high: Math.max(lastCandle.high, newClose),
          low: Math.min(lastCandle.low, newClose),
          close: newClose,
        })
      }
      lastCandle.high = Math.max(lastCandle.high, newClose)
      lastCandle.low = Math.min(lastCandle.low, newClose)
      lastCandle.close = newClose
    }

    // Update MA on live tick (skip for weekly — no meaningful real-time bucketing)
    if (!isLineChart && interval !== '1w' && ma3SeriesRef.current && ma5SeriesRef.current) {
      const lastIndex = currentData.length - 1
      const updateTime = currentData[lastIndex].time
      if (lastIndex >= 2) {
        let sum3 = 0
        for (let j = 0; j < 3; j++) sum3 += currentData[lastIndex - j].close
        ma3SeriesRef.current.update({ time: updateTime, value: sum3 / 3 })
      }
      if (lastIndex >= 4) {
        let sum5 = 0
        for (let j = 0; j < 5; j++) sum5 += currentData[lastIndex - j].close
        ma5SeriesRef.current.update({ time: updateTime, value: sum5 / 5 })
      }
      if (lastIndex >= 9) {
        let sum10 = 0
        for (let j = 0; j < 10; j++) sum10 += currentData[lastIndex - j].close
        ma10SeriesRef.current?.update({ time: updateTime, value: sum10 / 10 })
      }

      // Update KDJ on live tick using full data for continuity
      if (kSeriesRef.current && dSeriesRef.current && jSeriesRef.current) {
        const kdj = calculateKDJData(currentData)
        const lastK = kdj.k[kdj.k.length - 1]
        const lastD = kdj.d[kdj.d.length - 1]
        const lastJ = kdj.j[kdj.j.length - 1]
        if (lastK) { kSeriesRef.current.update(lastK); kdjDataRef.current.k[kdjDataRef.current.k.length - 1] = lastK }
        if (lastD) { dSeriesRef.current.update(lastD); kdjDataRef.current.d[kdjDataRef.current.d.length - 1] = lastD }
        if (lastJ) { jSeriesRef.current.update(lastJ); kdjDataRef.current.j[kdjDataRef.current.j.length - 1] = lastJ }
      }
    }
  }, [liveTick, interval, isLineChart])

  // Handler to scroll both charts to the latest data
  const handleGoToLatest = useCallback(() => {
    programmaticScrollRef.current = true
    const hasKdjData = kdjDataRef.current.k.length > 0
    if (hasKdjData && kdjChartRef.current) {
      kdjChartRef.current.timeScale().fitContent()
    }
    if (chartRef.current) {
      chartRef.current.timeScale().fitContent()
    }
    setShowGoToLatest(false)
    // Allow detection again after the animation settles
    setTimeout(() => { programmaticScrollRef.current = false }, 300)
  }, [])

  return (
    <div className="pb-[1em]">
      <div className="flex items-center gap-2 mb-2 overflow-x-auto no-scrollbar pb-1">
        {['1s', '5s', '10s', '1m', '2m', '3m', '5m', '15m', '1h', '4h', '1d', '1w'].map((i) => (
          <button
            key={i}
            onClick={() => onIntervalChange(i)}
            className={`px-3 py-1 text-xs rounded whitespace-nowrap ${interval === i ? 'bg-blue-600 text-white' : 'bg-[var(--bg-raised)] text-[var(--text-secondary)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)]'}`}
          >
            {i}
          </button>
        ))}
        {/* Spacer to push "Go to Latest" button to the right */}
        <div className="flex-1" />
        {showGoToLatest && (
          <button
            onClick={handleGoToLatest}
            className="flex items-center gap-1.5 px-3 py-1 text-xs font-medium text-white bg-blue-600 hover:bg-blue-500 rounded shadow transition-all duration-200 hover:scale-105 active:scale-95 animate-pulse"
            title="回到最新数据"
          >
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-3.5 h-3.5">
              <path fillRule="evenodd" d="M10 18a.75.75 0 01-.75-.75V4.66L7.3 6.76a.75.75 0 11-1.1-1.02l3.25-3.5a.75.75 0 011.1 0l3.25 3.5a.75.75 0 01-1.1 1.02l-1.95-2.1v12.59A.75.75 0 0110 18z" clipRule="evenodd" transform="rotate(90 10 10)" />
            </svg>
            回到最新 ▶
          </button>
        )}
      </div>
      <div className="relative" style={{ touchAction: 'none' }}>
        <div ref={mainContainerRef} />
        <div
          ref={tooltipRef}
          className="absolute z-10 pointer-events-none border rounded pt-1 pb-2 px-3 shadow-2xl hidden backdrop-blur-sm max-md:!hidden"
          style={{ top: 0, left: 0, backgroundColor: 'var(--bg-elevated)', borderColor: 'var(--border)' }}
        />
        <div
          ref={mobileInfoRef}
          className="absolute z-10 pointer-events-none hidden rounded p-1.5 shadow-lg md:!hidden"
          style={{ top: 2, left: 2, backgroundColor: 'color-mix(in srgb, var(--bg-elevated) 92%, transparent)', borderColor: 'var(--border)' }}
        />
      </div>
      {!isLineChart && <div ref={kdjContainerRef} className="mt-1 border-t border-gray-800" style={{ touchAction: 'none' }} />}
    </div>
  )
}
