import { createContext, useContext, useState, useCallback, ReactNode } from 'react'
import { useColorScheme } from 'react-native'

export interface ThemeColors {
  background: string
  elevated: string
  surface: string
  raised: string
  hover: string
  tooltip: string
  dangerBg: string
  textPrimary: string
  textSecondary: string
  textMuted: string
  textHeading: string
  border: string
  borderLight: string
  borderDarker: string
  ringSubtle: string
}

const darkTheme: ThemeColors = {
  background: '#0f1117',
  elevated: '#151924',
  surface: '#1f2937',
  raised: '#374151',
  hover: '#4b5563',
  tooltip: 'rgba(31,41,55,0.95)',
  dangerBg: 'rgba(127,29,29,0.2)',
  textPrimary: '#e2e8f0',
  textSecondary: '#9ca3af',
  textMuted: '#6b7280',
  textHeading: '#d1d5db',
  border: '#374151',
  borderLight: '#1f2937',
  borderDarker: '#4b5563',
  ringSubtle: 'rgba(255,255,255,0.05)',
}

const lightTheme: ThemeColors = {
  background: '#f3f4f6',
  elevated: '#ffffff',
  surface: '#ffffff',
  raised: '#e5e7eb',
  hover: '#d1d5db',
  tooltip: 'rgba(255,255,255,0.95)',
  dangerBg: 'rgba(254,202,202,0.3)',
  textPrimary: '#111827',
  textSecondary: '#4b5563',
  textMuted: '#9ca3af',
  textHeading: '#374151',
  border: '#e5e7eb',
  borderLight: '#f3f4f6',
  borderDarker: '#d1d5db',
  ringSubtle: 'rgba(0,0,0,0.05)',
}

interface ThemeContextValue {
  theme: 'dark' | 'light'
  colors: ThemeColors
  toggleTheme: () => void
}

const ThemeContext = createContext<ThemeContextValue>({
  theme: 'dark',
  colors: darkTheme,
  toggleTheme: () => {},
})

export function ThemeProvider({ children }: { children: ReactNode }) {
  const systemScheme = useColorScheme()
  const [isDark, setIsDark] = useState(systemScheme !== 'light')

  const colors = isDark ? darkTheme : lightTheme
  const toggleTheme = useCallback(() => setIsDark(p => !p), [])

  return (
    <ThemeContext.Provider value={{ theme: isDark ? 'dark' : 'light', colors, toggleTheme }}>
      {children}
    </ThemeContext.Provider>
  )
}

export function useTheme() {
  return useContext(ThemeContext)
}
