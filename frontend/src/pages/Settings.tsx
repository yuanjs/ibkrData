import { useEffect, useState } from 'react'
import { api } from '../api/client'

export function Settings() {
  const [settings, setSettings] = useState<Record<string, string>>({})
  const [alerts, setAlerts] = useState<unknown[]>([])
  const [newAlert, setNewAlert] = useState({ symbol: '', alert_type: 'price_above', threshold: '' })

  useEffect(() => {
    api.get<Record<string, string>>('/settings').then(d => { if (d) setSettings(d) })
    api.get<unknown[]>('/alerts').then(d => { if (Array.isArray(d)) setAlerts(d) })
  }, [])

  const [saved, setSaved] = useState(false)

  const save = async () => {
    await api.put('/settings', settings)
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  const addAlert = async () => {
    await api.post('/alerts', { ...newAlert, threshold: parseFloat(newAlert.threshold) })
    api.get('/alerts').then(d => setAlerts(d as unknown[]))
  }

  const delAlert = async (id: number) => {
    await api.del(`/alerts/${id}`)
    setAlerts(a => (a as Array<Record<string, unknown>>).filter(x => x.id !== id))
  }

  const field = (key: string, label: string, type = 'text') => (
    <div key={key}>
      <label className="text-xs block mb-1" style={{ color: 'var(--text-secondary)' }}>{label}</label>
      <input type={type} value={settings[key] ?? ''} onChange={e => setSettings(s => ({ ...s, [key]: e.target.value }))}
        className="border rounded px-3 py-1.5 text-sm w-full outline-none focus:ring-2 focus:ring-blue-500"
        style={{ backgroundColor: 'var(--bg-surface)', color: 'var(--text-primary)', borderColor: 'var(--border-darker)' }} />
    </div>
  )

  return (
    <div className="p-4 space-y-8 max-w-2xl">
      <section className="space-y-3">
        <h2 className="font-medium" style={{ color: 'var(--text-heading)' }}>IBKR 连接配置</h2>
        <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
          IBKR 网关地址和端口由 <code className="px-1 py-0.5 rounded" style={{ backgroundColor: 'var(--bg-surface)', border: '1px solid var(--border-darker)' }}>.env</code> 文件中的 <code>IB_HOST</code>、<code>IB_PORT</code>、<code>IB_CLIENT_ID</code> 管理，修改后需重启 collector 服务生效。
        </p>
      </section>

      <section className="space-y-3">
        <h2 className="font-medium" style={{ color: 'var(--text-heading)' }}>数据采集配置</h2>
        {field('account_refresh_interval', '账户刷新间隔（秒）', 'number')}
        {field('tick_retention_days', '原始数据保留天数', 'number')}
      </section>

      <section className="space-y-3">
        <h2 className="font-medium" style={{ color: 'var(--text-heading)' }}>前端显示配置</h2>
        {field('default_chart_interval', '默认K线周期')}
        {field('ui_language', '语言 (zh/en)')}
        {field('ui_timezone', '时区')}
      </section>

      <button onClick={save} className="px-6 py-2 bg-blue-600 rounded hover:bg-blue-500 text-sm text-white">
        {saved ? '已保存 ✓' : '保存设置'}
      </button>

      <section className="space-y-3">
        <h2 className="font-medium" style={{ color: 'var(--text-heading)' }}>告警规则</h2>
        <div className="flex gap-2 flex-wrap">
          <input placeholder="标的" value={newAlert.symbol} onChange={e => setNewAlert(a => ({ ...a, symbol: e.target.value }))}
            className="border rounded px-3 py-1.5 text-sm w-24 outline-none focus:ring-2 focus:ring-blue-500"
            style={{ backgroundColor: 'var(--bg-surface)', color: 'var(--text-primary)', borderColor: 'var(--border-darker)' }} />
          <select value={newAlert.alert_type} onChange={e => setNewAlert(a => ({ ...a, alert_type: e.target.value }))}
            className="border rounded px-3 py-1.5 text-sm outline-none focus:ring-2 focus:ring-blue-500"
            style={{ backgroundColor: 'var(--bg-surface)', color: 'var(--text-primary)', borderColor: 'var(--border-darker)' }}>
            <option value="price_above">价格高于</option>
            <option value="price_below">价格低于</option>
            <option value="daily_loss">日亏损超过</option>
          </select>
          <input type="number" placeholder="阈值" value={newAlert.threshold} onChange={e => setNewAlert(a => ({ ...a, threshold: e.target.value }))}
            className="border rounded px-3 py-1.5 text-sm w-28 outline-none focus:ring-2 focus:ring-blue-500"
            style={{ backgroundColor: 'var(--bg-surface)', color: 'var(--text-primary)', borderColor: 'var(--border-darker)' }} />
          <button onClick={addAlert} className="px-3 py-1.5 text-sm bg-green-700 rounded hover:bg-green-600 text-white">添加</button>
        </div>
        <table className="w-full text-sm">
          <thead><tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
            <th className="text-left py-2 px-3">标的</th>
            <th className="text-left py-2 px-3">类型</th>
            <th className="text-right py-2 px-3">阈值</th>
            <th className="py-2 px-3"></th>
          </tr></thead>
          <tbody>{(alerts as Array<Record<string, unknown>>).map(a => (
            <tr key={a.id as number} className="border-b" style={{ borderColor: 'var(--border-light)' }}>
              <td className="py-2 px-3 font-mono" style={{ color: 'var(--text-primary)' }}>{a.symbol as string ?? '-'}</td>
              <td className="py-2 px-3" style={{ color: 'var(--text-secondary)' }}>{a.alert_type as string}</td>
              <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{a.threshold as number}</td>
              <td className="py-2 px-3 text-right">
                <button onClick={() => delAlert(a.id as number)} className="text-red-400 hover:text-red-300 text-xs">删除</button>
              </td>
            </tr>
          ))}</tbody>
        </table>
      </section>
    </div>
  )
}
