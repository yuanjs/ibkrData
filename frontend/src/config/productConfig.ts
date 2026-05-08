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
