import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { StatusBar } from './components/StatusBar'
import { WebSocketProvider } from './components/WebSocketProvider'
import { Monitor } from './pages/Monitor'
import { Account } from './pages/Account'
import { Orders } from './pages/Orders'
import { History } from './pages/History'
import { Settings } from './pages/Settings'

const nav = [
  { to: '/', label: '实时监控' },
  { to: '/account', label: '账户' },
  { to: '/orders', label: '订单' },
  { to: '/history', label: '历史' },
  { to: '/settings', label: '设置' },
]

export default function App() {
  return (
    <BrowserRouter>
      <WebSocketProvider />
      <div className="flex flex-col h-screen bg-gray-900 text-gray-100">
        <StatusBar />
        <nav className="flex gap-1 px-4 py-2 bg-gray-800 border-b border-gray-700 overflow-x-auto no-scrollbar">
          {nav.map(({ to, label }) => (
            <NavLink key={to} to={to} end={to === '/'}
              className={({ isActive }) =>
                `px-3 py-1.5 text-sm rounded whitespace-nowrap ${isActive ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-700'}`
              }>
              {label}
            </NavLink>
          ))}
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
