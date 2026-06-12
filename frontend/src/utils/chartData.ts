export interface CandleLike {
  time: string | number
  open: number
  high: number
  low: number
  close: number
  volume?: number | null
  [key: string]: unknown
}

export function intervalSeconds(interval: string) {
  if (interval === '1d') return 86400
  if (interval === '1w') return 7 * 86400
  if (interval.endsWith('min')) return Number.parseInt(interval, 10) * 60
  if (interval.endsWith('m')) return Number.parseInt(interval, 10) * 60
  if (interval.endsWith('h')) return Number.parseInt(interval, 10) * 3600
  if (interval.endsWith('s')) return Number.parseInt(interval, 10)
  return 60
}

export function toChartTime(value: string | number) {
  return typeof value === 'string'
    ? Math.floor(new Date(value).getTime() / 1000)
    : value
}

const FUTURES_PRODUCT_CONFIGS: Record<string, { timezone: string; rollHour: number; rollMinute: number }> = {
  ASX200: { timezone: 'Australia/Sydney', rollHour: 17, rollMinute: 10 },
  WALLSTREET: { timezone: 'America/Chicago', rollHour: 16, rollMinute: 0 },
  NIKKEI_MINI: { timezone: 'Asia/Tokyo', rollHour: 16, rollMinute: 30 },
  USDJPY: { timezone: 'America/New_York', rollHour: 17, rollMinute: 0 },
  AUDUSD: { timezone: 'America/New_York', rollHour: 17, rollMinute: 0 },
  US10Y: { timezone: 'America/Chicago', rollHour: 16, rollMinute: 0 },
  CORN: { timezone: 'America/Chicago', rollHour: 16, rollMinute: 0 },
  HG: { timezone: 'America/New_York', rollHour: 17, rollMinute: 0 },
  NAS100: { timezone: 'America/Chicago', rollHour: 16, rollMinute: 0 },
  SP500: { timezone: 'America/Chicago', rollHour: 16, rollMinute: 0 },
  MICRO_DOW: { timezone: 'America/Chicago', rollHour: 16, rollMinute: 0 },
}

function normalizeFuturesSymbol(symbol: string) {
  const s = symbol.toUpperCase()
  if (s === 'SPI' || s === 'AP') return 'ASX200'
  if (s === 'MYM') return 'MICRO_DOW'
  if (s === 'YM' || s === 'DOW' || s === 'IX.D.DOW.IFA.IP' || s === 'DOW_MINI') return 'WALLSTREET'
  if (s === 'N225M' || s === '225M') return 'NIKKEI_MINI'
  if (s === 'USD.JPY') return 'USDJPY'
  if (s === 'AUD.USD') return 'AUDUSD'
  if (s === '10Y') return 'US10Y'
  if (s === 'ZC') return 'CORN'
  if (s === 'MNQ' || s === 'NQ') return 'NAS100'
  if (s === 'MES' || s === 'ES') return 'SP500'
  return s
}

function isWeekendInTimeZone(date: Date, timeZone: string) {
  const weekday = new Intl.DateTimeFormat('en-US', { timeZone, weekday: 'short' }).format(date)
  return weekday === 'Sat' || weekday === 'Sun'
}

function nextBusinessDayAfter(y: number, m: number, d: number, timeZone: string) {
  let candidate = new Date(Date.UTC(y, m - 1, d + 1, 12))
  while (isWeekendInTimeZone(candidate, timeZone)) {
    candidate = new Date(candidate.getTime() + 24 * 3600_000)
  }
  return candidate
}

export function getFuturesDailyAsOf(symbol: string, baseDate = new Date()) {
  const config = FUTURES_PRODUCT_CONFIGS[normalizeFuturesSymbol(symbol)]
  if (!config) return baseDate.toISOString()

  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: config.timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(baseDate)

  const get = (type: string) => Number(parts.find(p => p.type === type)?.value ?? 0)
  const hour = get('year') === 0 ? 0 : get('hour')
  const minute = get('minute')
  const y = get('year')
  const m = get('month')
  const d = get('day')

  const advanced = hour > config.rollHour || (hour === config.rollHour && minute >= config.rollMinute)
    ? nextBusinessDayAfter(y, m, d, config.timezone)
    : new Date(Date.UTC(y, m - 1, d, 12))

  return advanced.toISOString()
}

export function normalizeCandles(rows: CandleLike[], interval: string) {
  return rows.map((d) => {
    let t = toChartTime(d.time)
    if (interval === '1d') {
      const dt = new Date(t * 1000)
      t = Math.floor(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), 12) / 1000)
    }
    return { ...d, time: t }
  })
}

export function aggregateCandles(rows: CandleLike[], interval: string) {
  const seconds = intervalSeconds(interval)
  if (seconds <= 60 || interval === '1d') return rows

  const buckets = new Map<number, CandleLike & { time: number }>()
  for (const row of rows) {
    const t = toChartTime(row.time)
    const bucket = Math.floor(t / seconds) * seconds
    const existing = buckets.get(bucket)
    const volume = typeof row.volume === 'number' ? row.volume : 0
    if (!existing) {
      buckets.set(bucket, {
        ...row,
        time: bucket,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume,
      })
      continue
    }
    existing.high = Math.max(existing.high, row.high)
    existing.low = Math.min(existing.low, row.low)
    existing.close = row.close
    existing.volume = (existing.volume || 0) + volume
  }
  return Array.from(buckets.values()).sort((a, b) => Number(a.time) - Number(b.time))
}

function formatCsvCell(value: unknown) {
  if (value == null) return '""'
  const text = typeof value === 'number' && Number.isFinite(value) ? String(value) : String(value)
  return `"${text.replace(/"/g, '""')}"`
}

function formatCsvTime(value: string | number) {
  const seconds = typeof value === 'number' ? value : toChartTime(value)
  return new Date(seconds * 1000).toISOString()
}

export function candlesToCsv(rows: CandleLike[], symbol: string, interval: string) {
  const lines: (string | number | null | undefined)[][] = [
    ['symbol', symbol],
    ['interval', interval],
    [],
    ['time', 'open', 'high', 'low', 'close', 'volume'],
    ...rows.map(row => [
      formatCsvTime(row.time),
      row.open,
      row.high,
      row.low,
      row.close,
      row.volume ?? '',
    ]),
  ]

  return lines.map(line => line.map(formatCsvCell).join(',')).join('\n')
}
