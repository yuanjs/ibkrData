import { useState, useEffect, useCallback } from 'react'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { WebSocketProvider } from './components/WebSocketProvider'
import { useMarketStore } from './store/marketStore'
import { Monitor } from './pages/Monitor'
import { Account } from './pages/Account'
import { Orders } from './pages/Orders'
import { History } from './pages/History'
import { Settings } from './pages/Settings'
import { api } from './api/client'
import { getSymbolDescription } from './config/productConfig'

const nav = [
  { to: '/', label: '实时监控' },
  { to: '/account', label: '账户' },
  { to: '/orders', label: '订单' },
  { to: '/history', label: '历史' },
  { to: '/settings', label: '设置' },
]

function ConnectionDot() {
  const connected = useMarketStore(s => s.connected)
  return (
    <span
      className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${connected ? 'bg-green-400' : 'bg-red-400'}`}
      title={connected ? 'IBKR 已连接' : '断开连接 - 重连中...'}
    />
  )
}

function ThemeToggle({ theme, onToggle }: { theme: string; onToggle: () => void }) {
  return (
    <button
      onClick={onToggle}
      className="p-2 rounded bg-[var(--bg-raised)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors border border-[var(--border)]"
      title={theme === 'dark' ? '切换到浅色模式' : '切换到深色模式'}
    >
      {theme === 'dark' ? (
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5">
          <path fillRule="evenodd" d="M10 2a.75.75 0 01.75.75v1.5a.75.75 0 01-1.5 0v-1.5A.75.75 0 0110 2zM10 15a.75.75 0 01.75.75v1.5a.75.75 0 01-1.5 0v-1.5A.75.75 0 0110 15zM10 7a3 3 0 100 6 3 3 0 000-6zM15.657 5.404a.75.75 0 10-1.06-1.06l-1.061 1.06a.75.75 0 001.06 1.06l1.06-1.06zM6.464 14.596a.75.75 0 10-1.06-1.06l-1.06 1.06a.75.75 0 001.06 1.06l1.06-1.06zM18 10a.75.75 0 01-.75.75h-1.5a.75.75 0 010-1.5h1.5A.75.75 0 0118 10zM5 10a.75.75 0 01-.75.75h-1.5a.75.75 0 010-1.5h1.5A.75.75 0 015 10zM14.596 15.657a.75.75 0 001.06-1.06l-1.06-1.06a.75.75 0 10-1.06 1.06l1.06 1.06zM5.404 6.464a.75.75 0 001.06-1.06l-1.06-1.06a.75.75 0 10-1.06 1.06l1.06 1.06z" clipRule="evenodd" />
        </svg>
      ) : (
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5">
          <path fillRule="evenodd" d="M7.455 2.004a.75.75 0 01.26.77 7 7 0 009.958 7.967.75.75 0 011.067.853A8.5 8.5 0 116.647 1.921a.75.75 0 01.808.083z" clipRule="evenodd" />
        </svg>
      )}
    </button>
  )
}

export default function App() {
  const [theme, setTheme] = useState(() => {
    const saved = localStorage.getItem('theme')
    if (saved) return saved
    return window.innerWidth < 768 ? 'light' : 'dark'
  })

  const activeSymbol = useMarketStore(s => s.activeSymbol)
  const setActiveSymbol = useMarketStore(s => s.setActiveSymbol)
  const quotes = useMarketStore(s => s.quotes)
  const initQuotes = useMarketStore(s => s.initQuotes)
  const symbols = Object.keys(quotes)

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  useEffect(() => {
    api.get<any[]>('/symbols').then(data => {
      if (Array.isArray(data)) {
        const symList = data.map(s => s.symbol)
        initQuotes(symList)
        if (symList.length > 0 && !useMarketStore.getState().activeSymbol) {
          useMarketStore.getState().setActiveSymbol(symList[0])
        }
      }
    }).catch(err => console.error('Failed to fetch symbols:', err))
  }, [initQuotes])

  const toggleTheme = useCallback(() => {
    setTheme(t => t === 'dark' ? 'light' : 'dark')
  }, [])

  return (
    <BrowserRouter>
      <WebSocketProvider />
      <div className="flex flex-col h-screen overflow-hidden" style={{ backgroundColor: 'var(--bg-base)', color: 'var(--text-primary)' }}>

        {/* Desktop Navigation */}
        <nav className="hidden md:flex gap-1 px-4 py-2 overflow-x-auto no-scrollbar" style={{ backgroundColor: 'var(--bg-surface)', borderBottom: '1px solid var(--border)' }}>
          {nav.map(({ to, label }) => (
            <NavLink key={to} to={to} end={to === '/'}
              className={({ isActive }) =>
                `px-3 py-1.5 text-sm rounded whitespace-nowrap ${isActive ? 'bg-blue-600 text-white' : 'text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-raised)]'}`
              }>
              {label}
            </NavLink>
          ))}
          <div className="ml-auto flex items-center gap-2">
            <ConnectionDot />
            <ThemeToggle theme={theme} onToggle={toggleTheme} />
          </div>
        </nav>

        {/* Mobile Navigation / Symbol Selector */}
        <nav className="flex md:hidden items-center gap-2 px-2 py-1.5" style={{ backgroundColor: 'var(--bg-surface)', borderBottom: '1px solid var(--border)' }}>
          <ConnectionDot />
          <div className="flex-1 relative">
            <select
              value={activeSymbol || ''}
              onChange={(e) => setActiveSymbol(e.target.value)}
              className="w-full h-8 border rounded-lg px-2 outline-none focus:ring-2 focus:ring-blue-500 appearance-none font-mono text-sm pr-7"
              style={{
                backgroundColor: 'var(--bg-elevated)',
                color: 'var(--text-primary)',
                borderColor: 'var(--border)',
              }}
            >
              <option value="" disabled>-- 请选择标的 --</option>
              {symbols.map(sym => (
                <option key={sym} value={sym}>
                  {sym} {getSymbolDescription(sym) ? `(${getSymbolDescription(sym)})` : ''}
                </option>
              ))}
            </select>
            <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none text-[var(--text-secondary)]">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4">
                <path fillRule="evenodd" d="M5.22 8.22a.75.75 0 011.06 0L10 11.94l3.72-3.72a.75.75 0 111.06 1.06l-4.25 4.25a.75.75 0 01-1.06 0L5.22 9.28a.75.75 0 010-1.06z" clipRule="evenodd" />
              </svg>
            </div>
          </div>
          <ThemeToggle theme={theme} onToggle={toggleTheme} />
        </nav>

        <main className="flex-1 overflow-auto">
          <Routes>
            <Route path="/" element={<Monitor />} />
            <Route path="/account" element={<Account />} />
            <Route path="/orders" element={<Orders />} />
            <Route path="/history" element={<History />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
