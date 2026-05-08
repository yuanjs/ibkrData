import { useEffect, useRef, useCallback, useState } from 'react'
import { createChart, CandlestickSeries, LineSeries, type IChartApi, type ISeriesApi, type CandlestickData } from 'lightweight-charts'
import { getProductConfig } from '../config/productConfig'

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
  if (inv.endsWith('min')) return parseInt(inv) * 60
  if (inv.endsWith('m')) return parseInt(inv) * 60
  if (inv.endsWith('h')) return parseInt(inv) * 3600
  if (inv.endsWith('s')) return parseInt(inv)
  return 60
}

// Helper to get the start of the day in a specific timezone
function getDailyBucketTime(tickTimeSec: number, timezone: string) {
  const date = new Date(tickTimeSec * 1000)
  // Use Intl.DateTimeFormat to get the date parts in the target timezone
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  })
  const parts = formatter.format(date).split('-') // YYYY-MM-DD

  // Construct a new date object representing the start of that day in that timezone
  // We need to find the UTC timestamp that corresponds to YYYY-MM-DD 00:00:00 in the target timezone
  // A simple way is to use the same Intl trick or manually calculate offset,
  // but simpler is to use a date string and parse it as a local time then adjust,
  // or use the fact that lightweight-charts '1d' can take a string 'YYYY-MM-DD'.
  // However, the rest of the app uses numeric timestamps.

  // Create a string that represents the midnight of that day in the given timezone
  const dateString = `${parts[0]}-${parts[1]}-${parts[2]}T00:00:00`

  // To get the UTC timestamp of "YYYY-MM-DD 00:00:00" in "timezone":
  // We can use the fact that new Date(dateString).toLocaleString(...) should match the dateString if the local TZ is the same.
  // A more robust way:
  const tempDate = new Date(dateString) // This is local time
  // This is a bit tricky in JS without a library like luxon.
  // Let's use a simpler approach: get the "YYYY-MM-DD" and use it as the 'time' for lightweight-charts if needed,
  // or just return the UTC timestamp of the start of that day.

  // Alternative: return an object { time: number | string }
  return `${parts[0]}-${parts[1]}-${parts[2]}`
}

export function CandleChart({ symbol, data, liveTick, interval, onIntervalChange }: Props) {
  const mainContainerRef = useRef<HTMLDivElement>(null)
  const kdjContainerRef = useRef<HTMLDivElement>(null)
  const tooltipRef = useRef<HTMLDivElement>(null)

  const chartRef = useRef<IChartApi | undefined>(undefined)
  const seriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const ma3SeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)
  const ma5SeriesRef = useRef<ISeriesApi<any> | undefined>(undefined)

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
  // Ref to hold the initial data length snapshot when data is first loaded
  // This avoids the button flickering when live ticks increase dataLength
  const initialDataLengthRef = useRef(0)

  const isLineChart = interval === '1s' || interval === '5s'

  const getTimezone = useCallback(() => {
    return getProductConfig(symbol || '').timezone
  }, [symbol])

  // Format time for display
  const formatTime = useCallback((timeSec: number) => {
    const date = new Date(timeSec * 1000)
    if (isNaN(date.getTime())) return String(timeSec)
    const tz = getTimezone()
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
  }, [getTimezone])

  // Create / destroy charts
  useEffect(() => {
    if (!mainContainerRef.current) return

    const tz = getTimezone()

    // Create main chart
    const chart = createChart(mainContainerRef.current, {
      layout: { background: { color: '#0f1117' }, textColor: '#9ca3af' },
      grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
      width: mainContainerRef.current.clientWidth,
      height: 320,
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
      timeScale: {
        timeVisible: true,
        secondsVisible: true,
        tickMarkFormatter: (time: any) => {
          const date = new Date(time * 1000)
          if (isNaN(date.getTime())) return ''
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
      })
    } else {
      series = chart.addSeries(CandlestickSeries, {
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderUpColor: '#22c55e',
        borderDownColor: '#ef4444',
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444',
      })
    }
    seriesRef.current = series

    // MA series (only for candle charts)
    if (!isLineChart) {
      ma3SeriesRef.current = chart.addSeries(LineSeries, {
        color: '#facc15',
        lineWidth: 1,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      })
      ma5SeriesRef.current = chart.addSeries(LineSeries, {
        color: '#ec4899',
        lineWidth: 1,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      })
    } else {
      ma3SeriesRef.current = undefined
      ma5SeriesRef.current = undefined
    }

    // KDJ chart (only for candle charts)
    let kdjChart: IChartApi | undefined
    if (!isLineChart && kdjContainerRef.current) {
      kdjChart = createChart(kdjContainerRef.current, {
        layout: { background: { color: '#0f1117' }, textColor: '#9ca3af' },
        grid: { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
        width: mainContainerRef.current.clientWidth,
        height: 120,
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
        timeScale: {
          visible: false,
        },
        crosshair: {
          vertLine: { visible: true },
          horzLine: { visible: true },
        },
      })
      kdjChartRef.current = kdjChart

      kSeriesRef.current = kdjChart.addSeries(LineSeries, {
        color: '#fff', lineWidth: 1, title: '',
        lastValueVisible: true, priceLineVisible: false,
      })
      dSeriesRef.current = kdjChart.addSeries(LineSeries, {
        color: '#facc15', lineWidth: 1, title: '',
        lastValueVisible: true, priceLineVisible: false,
      })
      jSeriesRef.current = kdjChart.addSeries(LineSeries, {
        color: '#ec4899', lineWidth: 1, title: '',
        lastValueVisible: true, priceLineVisible: false,
      })

      // Add 0 and 100 reference lines
      kSeriesRef.current.createPriceLine({ price: 0, color: '#4b5563', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '' })
      kSeriesRef.current.createPriceLine({ price: 100, color: '#4b5563', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '' })

      // Sync scroll/zoom between main chart and KDJ chart using TIME range (not logical index)
      // This ensures alignment even though KDJ has fewer bars than the main chart
      let syncing = false
      chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
        if (syncing || !range || !kdjChartRef.current) return
        syncing = true
        try { kdjChartRef.current.timeScale().setVisibleRange(range as any) } catch { }
        syncing = false
      })
      kdjChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
        if (syncing || !range || !chartRef.current) return
        syncing = true
        try { chartRef.current.timeScale().setVisibleRange(range as any) } catch { }
        syncing = false
      })

      // Sync "Go to Latest" visibility between charts
      kdjChart.timeScale().subscribeVisibleLogicalRangeChange(() => {
        // Do nothing — main chart drives the button state
      })

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
      if (!tt || !mainContainerRef.current) return

      if (
        param.point === undefined ||
        !param.time ||
        param.point.x < 0 ||
        param.point.x > mainContainerRef.current.clientWidth ||
        param.point.y < 0 ||
        param.point.y > 320
      ) {
        tt.style.display = 'none'
        return
      }

      const sData = param.seriesData.get(series) as any
      if (!sData) {
        tt.style.display = 'none'
        return
      }

      tt.style.display = 'block'
      const timeSec = typeof sData.time === 'number' ? sData.time : 0
      const timeStr = formatTime(timeSec)

      // Lookup MA values safely
      const ma3Val = ma3SeriesRef.current ? (param.seriesData.get(ma3SeriesRef.current) as any)?.value : undefined
      const ma5Val = ma5SeriesRef.current ? (param.seriesData.get(ma5SeriesRef.current) as any)?.value : undefined

      // Lookup KDJ values
      const kVal = kdjDataRef.current.k.find((x) => x.time === timeSec)?.value
      const dVal = kdjDataRef.current.d.find((x) => x.time === timeSec)?.value
      const jVal = kdjDataRef.current.j.find((x) => x.time === timeSec)?.value

      if (isLineChart) {
        tt.innerHTML = `
          <div class="font-bold text-gray-200 text-sm whitespace-nowrap">${timeStr}</div>
          <div class="mt-1 text-xs"><span class="text-gray-400">Price:</span><span class="text-blue-400 ml-2 font-mono">${(sData.value ?? sData.close)?.toFixed(2) ?? '-'}</span></div>
        `
      } else {
        tt.innerHTML = `
          <div class="font-bold text-gray-200 text-sm whitespace-nowrap">${timeStr}</div>
          <div class="grid grid-cols-2 gap-x-3 gap-y-1 text-xs mt-2">
            <div class="flex justify-between w-16"><span class="text-gray-400">O:</span><span class="${sData.open > sData.close ? 'text-red-400' : 'text-green-400'}">${sData.open?.toFixed(2)}</span></div>
            <div class="flex justify-between w-16"><span class="text-gray-400">H:</span><span class="${sData.high > sData.close ? 'text-red-400' : 'text-green-400'}">${sData.high?.toFixed(2)}</span></div>
            <div class="flex justify-between w-16"><span class="text-gray-400">L:</span><span class="${sData.low > sData.close ? 'text-red-400' : 'text-green-400'}">${sData.low?.toFixed(2)}</span></div>
            <div class="flex justify-between w-16"><span class="text-gray-400">C:</span><span class="${sData.close >= sData.open ? 'text-green-400' : 'text-red-400'}">${sData.close?.toFixed(2)}</span></div>
          </div>
          <div class="flex gap-4 mt-2 text-[10px] font-mono">
            <div class="flex items-center gap-1"><div class="w-2 h-0.5 bg-[#facc15]"></div><span class="text-gray-400">MA3:</span><span class="text-yellow-400">${ma3Val?.toFixed(2) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><div class="w-2 h-0.5 bg-[#ec4899]"></div><span class="text-gray-400">MA5:</span><span class="text-pink-400">${ma5Val?.toFixed(2) ?? '-'}</span></div>
          </div>
          <div class="flex gap-3 mt-1 text-[10px] font-mono border-t border-gray-700 pt-1">
            <div class="flex items-center gap-1"><span class="text-gray-400">K:</span><span class="text-white">${kVal?.toFixed(2) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><span class="text-gray-400">D:</span><span class="text-yellow-400">${dVal?.toFixed(2) ?? '-'}</span></div>
            <div class="flex items-center gap-1"><span class="text-gray-400">J:</span><span class="text-pink-400">${jVal?.toFixed(2) ?? '-'}</span></div>
          </div>
        `
      }

      // Positioning logic to keep tooltip within view
      const ttWidth = tt.offsetWidth
      const ttHeight = tt.offsetHeight
      const x = Math.min(Math.max(0, param.point.x + 15), mainContainerRef.current.clientWidth - ttWidth - 5)
      const y = Math.min(Math.max(10, param.point.y - ttHeight / 2), 320 - ttHeight - 5)
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

    return () => {
      resizeObserver.disconnect()
      chart.remove()
      chartRef.current = undefined
      seriesRef.current = undefined
      ma3SeriesRef.current = undefined
      ma5SeriesRef.current = undefined
      if (kdjChart) {
        kdjChart.remove()
      }
      kdjChartRef.current = undefined
      kSeriesRef.current = undefined
      dSeriesRef.current = undefined
      jSeriesRef.current = undefined
    }
  }, [interval, symbol, isLineChart, getTimezone, formatTime])

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
      }
      ma3SeriesRef.current?.setData(ma3Data)
      ma5SeriesRef.current?.setData(ma5Data)

      // Calculate KDJ
      if (kSeriesRef.current && dSeriesRef.current && jSeriesRef.current) {
        const kdj = calculateKDJData(normalizedData)
        kdjDataRef.current = kdj
        kSeriesRef.current.setData(kdj.k)
        dSeriesRef.current.setData(kdj.d)
        jSeriesRef.current.setData(kdj.j)
      }
    }

    // Fit all loaded candles into the visible area
    // This fixes the issue where switching intervals shows only 2 candles instead of all
    // Use programmaticScrollRef to suppress false detection in the range change listener
    programmaticScrollRef.current = true
    if (chartRef.current) {
      chartRef.current.timeScale().fitContent()
    }
    if (kdjChartRef.current) {
      kdjChartRef.current.timeScale().fitContent()
    }
    // Reset the "Go to Latest" button state
    setShowGoToLatest(false)
    // Allow detection again after a short delay (let the range change event settle)
    setTimeout(() => { programmaticScrollRef.current = false }, 300)
  }, [data, isLineChart])

  // Process live ticks
  useEffect(() => {
    if (!liveTick?.price || lastDataRef.current.length === 0 || !seriesRef.current) return

    const currentData = lastDataRef.current
    const lastCandle = currentData[currentData.length - 1] as any
    const newClose = liveTick.price

    const tickTimeSec = liveTick.time
      ? Math.floor(new Date(liveTick.time).getTime() / 1000)
      : Math.floor(Date.now() / 1000)

    let currentBucketTime: number | string
    if (interval === '1d') {
      currentBucketTime = getDailyBucketTime(tickTimeSec, getTimezone())
    } else {
      const invSec = getIntervalSeconds(interval)
      currentBucketTime = tickTimeSec - (tickTimeSec % invSec)
    }

    // Comparison logic for new candle
    const isNewCandle = interval === '1d'
      ? (currentBucketTime !== lastCandle.time)
      : (Number(currentBucketTime) > Number(lastCandle.time))

    if (isNewCandle) {
      const newCandle = { time: currentBucketTime, open: newClose, high: newClose, low: newClose, close: newClose }
      if (isLineChart) {
        seriesRef.current.update({ time: currentBucketTime as any, value: newClose })
      } else {
        seriesRef.current.update(newCandle as any)
      }
      currentData.push(newCandle)
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

    // Update MA on live tick
    if (!isLineChart && ma3SeriesRef.current && ma5SeriesRef.current) {
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

      // Update KDJ on live tick
      if (kSeriesRef.current && dSeriesRef.current && jSeriesRef.current) {
        const kdj = calculateKDJData(currentData.slice(-20))
        const lastK = kdj.k[kdj.k.length - 1]
        const lastD = kdj.d[kdj.d.length - 1]
        const lastJ = kdj.j[kdj.j.length - 1]
        if (lastK) kSeriesRef.current.update(lastK)
        if (lastD) dSeriesRef.current.update(lastD)
        if (lastJ) jSeriesRef.current.update(lastJ)
      }
    }
  }, [liveTick, interval, isLineChart])

  // Handler to scroll both charts to the latest data
  const handleGoToLatest = useCallback(() => {
    programmaticScrollRef.current = true
    if (chartRef.current) {
      chartRef.current.timeScale().fitContent()
    }
    if (kdjChartRef.current) {
      kdjChartRef.current.timeScale().fitContent()
    }
    setShowGoToLatest(false)
    // Allow detection again after the animation settles
    setTimeout(() => { programmaticScrollRef.current = false }, 300)
  }, [])

  return (
    <div>
      <div className="flex items-center gap-2 mb-2 overflow-x-auto no-scrollbar pb-1">
        {['1s', '5s', '1m', '3m', '5m', '15m', '1h', '4h', '1d'].map((i) => (
          <button
            key={i}
            onClick={() => onIntervalChange(i)}
            className={`px-3 py-1 text-xs rounded whitespace-nowrap ${interval === i ? 'bg-blue-600' : 'bg-gray-700 hover:bg-gray-600'}`}
          >
            {i}
          </button>
        ))}
        {/* Spacer to push "Go to Latest" button to the right */}
        <div className="flex-1" />
        {/* "Go to Latest" button — shown when user scrolls/zooms away from the latest data */}
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
      <div className="relative">
        <div ref={mainContainerRef} />
        <div
          ref={tooltipRef}
          className="absolute z-10 pointer-events-none bg-gray-800/95 border border-gray-600 rounded pt-1 pb-2 px-3 shadow-2xl hidden backdrop-blur-sm"
          style={{ top: 0, left: 0 }}
        />
      </div>
      {!isLineChart && <div ref={kdjContainerRef} className="mt-1 border-t border-gray-800" />}
    </div>
  )
}
