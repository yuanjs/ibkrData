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
};

export function getSymbolDescription(symbol: string): string {
  if (!symbol) return '';
  return SYMBOL_DESCRIPTIONS[symbol.toUpperCase()] || '';
}

export const PRODUCT_CONFIGS: Record<string, { timezone: string }> = {
  'ASX200': {
    timezone: 'Australia/Sydney',
  },
  'WALLSTREET': {
    timezone: 'America/Chicago',
  },
  'NIKKEI_MINI': {
    timezone: 'Asia/Tokyo',
  },
  'USDJPY': {
    timezone: 'America/New_York',
  }
};

export function getProductConfig(symbol: string) {
  const s = symbol ? symbol.toUpperCase() : 'ASX200';
  
  // Symbol normalization for mapped variants
  let normalized = s;
  if (s === 'SPI' || s === 'AP') normalized = 'ASX200';
  if (s === 'YM' || s === 'DOW' || s === 'IX.D.DOW.IFA.IP' || s === 'DOW_MINI' || s === 'MYM') normalized = 'WALLSTREET';
  if (s === 'N225M' || s === '225M') normalized = 'NIKKEI_MINI';
  
  return PRODUCT_CONFIGS[normalized] || PRODUCT_CONFIGS['ASX200'];
}
