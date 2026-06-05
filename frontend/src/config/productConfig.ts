export const SYMBOL_DESCRIPTIONS: Record<string, string> = {
  'SPI': '澳指',
  'AP': '澳指',
  'ASX200': '澳指',
  'MYM': '道指',
  'YM': '道指',
  'DOW': '道指',
  'DOW_MINI': '道指',
  'WALLSTREET': '道指',
  'N225M': '日经',
  '225M': '日经',
  'NIKKEI_MINI': '日经',
  'USD.JPY': '汇率',
  'USDJPY': '汇率',
  'AUD.USD': '澳美',
  'AUDUSD': '澳美',
  '10Y': '美债',
  'US10Y': '美债',
  'ZC': '玉米',
  'CORN': '玉米',
  'HG': '高级铜',
  'MNQ': '纳指',
  'NQ': '纳指',
  'MES': '标普',
  'ES': '标普',
};

export function getSymbolDescription(symbol: string): string {
  if (!symbol) return '';
  return SYMBOL_DESCRIPTIONS[symbol.toUpperCase()] || '';
}

interface ProductConfig {
  timezone: string
  rollHour: number
  rollMinute: number
  decimalPlaces?: number
  multiplier?: number   // 合约乘数，用于实时 PnL 计算
}

export const PRODUCT_CONFIGS: Record<string, ProductConfig> = {
  'ASX200': {
    timezone: 'Australia/Sydney',
    rollHour: 17,
    rollMinute: 10,
    multiplier: 25,
  },
  'WALLSTREET': {
    timezone: 'America/Chicago',
    rollHour: 16,
    rollMinute: 0,
    multiplier: 5,
  },
  'NIKKEI_MINI': {
    timezone: 'Asia/Tokyo',
    rollHour: 16,
    rollMinute: 30,
    multiplier: 100,
  },
  'USDJPY': {
    timezone: 'America/New_York',
    rollHour: 17,
    rollMinute: 0,
    decimalPlaces: 3,
    multiplier: 100000,
  },
  'AUDUSD': {
    timezone: 'America/New_York',
    rollHour: 17,
    rollMinute: 0,
    decimalPlaces: 5,
    multiplier: 100000,
  },
  'US10Y': {
    timezone: 'America/Chicago',
    rollHour: 16,
    rollMinute: 0,
    decimalPlaces: 3,
    multiplier: 1000,
  },
  'CORN': {
    timezone: 'America/Chicago',
    rollHour: 16,
    rollMinute: 0,
    multiplier: 50,
  },
  'HG': {
    timezone: 'America/Chicago',
    rollHour: 16,
    rollMinute: 0,
    decimalPlaces: 4,
    multiplier: 25000,
  },
  'NAS100': {
    timezone: 'America/Chicago',
    rollHour: 16,
    rollMinute: 0,
    multiplier: 2,
  },
  'SP500': {
    timezone: 'America/Chicago',
    rollHour: 16,
    rollMinute: 0,
    multiplier: 5,
  },

function normalizeSymbol(s: string): string {
  if (s === 'SPI' || s === 'AP') return 'ASX200'
  if (s === 'YM' || s === 'DOW' || s === 'IX.D.DOW.IFA.IP' || s === 'DOW_MINI' || s === 'MYM') return 'WALLSTREET'
  if (s === 'N225M' || s === '225M') return 'NIKKEI_MINI'
  if (s === 'USD.JPY') return 'USDJPY'
  if (s === 'AUD.USD') return 'AUDUSD'
  if (s === '10Y') return 'US10Y'
  if (s === 'ZC') return 'CORN'
  if (s === 'MNQ' || s === 'NQ') return 'NAS100'
  if (s === 'MES' || s === 'ES') return 'SP500'
  return s
}

export function getProductConfig(symbol: string): ProductConfig {
  const s = symbol ? symbol.toUpperCase() : 'ASX200'
  const normalized = normalizeSymbol(s)
  return PRODUCT_CONFIGS[normalized] || PRODUCT_CONFIGS['ASX200']
}

export function getSymbolDecimalPlaces(symbol: string | undefined): number {
  if (!symbol) return 2
  const s = symbol.toUpperCase()
  const normalized = normalizeSymbol(s)
  return PRODUCT_CONFIGS[normalized]?.decimalPlaces ?? 2
}
