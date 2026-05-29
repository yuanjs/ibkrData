import { useEffect, useState } from 'react'
import { View, Text, TextInput, TouchableOpacity, StyleSheet, ScrollView, Alert } from 'react-native'
import { api } from '../src/api/client'
import { useTheme } from '../src/theme'
import { getRuntimeConfig, setRuntimeConfig } from '../src/config/runtimeConfig'
const Field = ({ name, label, value, onChange, type }: { name: string; label: string; value: string; onChange: (v: string) => void; type?: 'text' | 'numeric' }) => {
  const { colors } = useTheme()
  return (
    <View style={styles.fieldRow}>
      <Text style={[styles.fieldLabel, { color: colors.textSecondary }]}>{label}</Text>
      <TextInput
        style={[styles.fieldInput, { backgroundColor: colors.surface, color: colors.textPrimary, borderColor: colors.borderDarker }]}
        value={value}
        onChangeText={onChange}
        keyboardType={type === 'numeric' ? 'numeric' : 'default'}
      />
    </View>
  )
}

export default function Settings() {
  const [settings, setSettings] = useState<Record<string, string>>({})
  const [alerts, setAlerts] = useState<unknown[]>([])
  const [newAlert, setNewAlert] = useState({ symbol: '', alert_type: 'price_above', threshold: '' })
  const [saved, setSaved] = useState(false)
  const { colors } = useTheme()

  // Server config state (from runtime config, modifiable)
  const rc = getRuntimeConfig()
  const [serverUrl, setServerUrl] = useState(rc.apiUrl)
  const [wsUrl, setWsUrl] = useState(rc.wsUrl)
  const [apiToken, setApiToken] = useState(rc.token)

  useEffect(() => {
    api.get<Record<string, string>>('/settings').then(d => { if (d) setSettings(d) }).catch(() => {})
    api.get<unknown[]>('/alerts').then(d => { if (Array.isArray(d)) setAlerts(d) }).catch(() => {})
  }, [])

  const saveServerConfig = () => {
    setRuntimeConfig({ apiUrl: serverUrl, wsUrl, token: apiToken })
    Alert.alert('提示', '服务器配置已更新，请返回首页重新连接。')
  }

  const save = async () => {
    try {
      await api.put('/settings', settings)
      setSaved(true)
      setTimeout(() => setSaved(false), 2000)
    } catch (e: any) {
      Alert.alert('保存失败', e.message)
    }
  }

  const addAlert = async () => {
    if (!newAlert.symbol || !newAlert.threshold) return
    try {
      await api.post('/alerts', { ...newAlert, threshold: parseFloat(newAlert.threshold) })
      setNewAlert({ symbol: '', alert_type: 'price_above', threshold: '' })
      const data = await api.get('/alerts')
      if (Array.isArray(data)) setAlerts(data)
    } catch (e: any) {
      Alert.alert('添加失败', e.message)
    }
  }

  const delAlert = async (id: number) => {
    try {
      await api.del(`/alerts/${id}`)
      setAlerts(a => (a as Array<Record<string, unknown>>).filter(x => x.id !== id))
    } catch (e: any) {
      Alert.alert('删除失败', e.message)
    }
  }

  return (
    <ScrollView style={[styles.container, { backgroundColor: colors.background }]} keyboardShouldPersistTaps="handled">
      {/* Server Connection Config */}
      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textHeading }]}>服务器连接（运行时配置，不随打包固化）</Text>
        <Field name="apiUrl" label="API 地址" value={serverUrl} onChange={setServerUrl} />
        <Field name="wsUrl" label="WebSocket 地址" value={wsUrl} onChange={setWsUrl} />
        <Field name="token" label="API Token" value={apiToken} onChange={setApiToken} />
        <TouchableOpacity onPress={saveServerConfig} style={styles.connectBtn}>
          <Text style={{ color: '#fff', fontSize: 13 }}>应用设置并重连</Text>
        </TouchableOpacity>
      </View>

      {/* IBKR Config Info */}
      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textHeading }]}>IBKR 连接配置</Text>
        <Text style={[styles.sectionDesc, { color: colors.textSecondary }]}>
          IBKR 网关地址和端口由环境变量 IB_HOST、IB_PORT、IB_CLIENT_ID 管理，修改后需重启 collector 服务生效。
        </Text>
      </View>

      {/* Data Collection */}
      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textHeading }]}>数据采集配置</Text>
        <Field name="account_refresh_interval" label="账户刷新间隔（秒）" value={settings['account_refresh_interval'] ?? ''} onChange={v => setSettings(s => ({ ...s, account_refresh_interval: v }))} type="numeric" />
        <Field name="tick_retention_days" label="原始数据保留天数" value={settings['tick_retention_days'] ?? ''} onChange={v => setSettings(s => ({ ...s, tick_retention_days: v }))} type="numeric" />
      </View>

      {/* Display Config */}
      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textHeading }]}>前端显示配置</Text>
        <Field name="default_chart_interval" label="默认K线周期" value={settings['default_chart_interval'] ?? ''} onChange={v => setSettings(s => ({ ...s, default_chart_interval: v }))} />
        <Field name="ui_language" label="语言 (zh/en)" value={settings['ui_language'] ?? ''} onChange={v => setSettings(s => ({ ...s, ui_language: v }))} />
        <Field name="ui_timezone" label="时区" value={settings['ui_timezone'] ?? ''} onChange={v => setSettings(s => ({ ...s, ui_timezone: v }))} />
      </View>

      <TouchableOpacity onPress={save} style={styles.saveBtn}>
        <Text style={{ color: '#fff', fontSize: 13 }}>{saved ? '已保存 ✓' : '保存设置'}</Text>
      </TouchableOpacity>

      {/* Alerts Section */}
      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textHeading }]}>告警规则</Text>

        <View style={styles.alertForm}>
          <TextInput
            style={[styles.alertInput, { backgroundColor: colors.surface, color: colors.textPrimary, borderColor: colors.borderDarker }]}
            placeholder="标的"
            placeholderTextColor={colors.textMuted}
            value={newAlert.symbol}
            onChangeText={v => setNewAlert(a => ({ ...a, symbol: v }))}
          />
          <View style={styles.alertTypeRow}>
            {['price_above', 'price_below', 'daily_loss'].map(t => (
              <TouchableOpacity
                key={t}
                onPress={() => setNewAlert(a => ({ ...a, alert_type: t }))}
                style={[styles.typeBtn, { backgroundColor: newAlert.alert_type === t ? '#2563eb' : colors.raised }]}
              >
                <Text style={{ color: newAlert.alert_type === t ? '#fff' : colors.textSecondary, fontSize: 11 }}>
                  {t === 'price_above' ? '价格高于' : t === 'price_below' ? '价格低于' : '日亏损超过'}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
          <TextInput
            style={[styles.alertInput, { backgroundColor: colors.surface, color: colors.textPrimary, borderColor: colors.borderDarker }]}
            placeholder="阈值"
            placeholderTextColor={colors.textMuted}
            value={newAlert.threshold}
            onChangeText={v => setNewAlert(a => ({ ...a, threshold: v }))}
            keyboardType="numeric"
          />
          <TouchableOpacity onPress={addAlert} style={styles.addBtn}>
            <Text style={{ color: '#fff', fontSize: 12 }}>添加</Text>
          </TouchableOpacity>
        </View>

        {/* Alerts table */}
        <View style={[styles.alertHeader, { borderBottomColor: colors.border }]}>
          <Text style={[styles.alertTh, { color: colors.textSecondary }]}>标的</Text>
          <Text style={[styles.alertTh, { color: colors.textSecondary }]}>类型</Text>
          <Text style={[styles.alertTh, styles.alertThRight, { color: colors.textSecondary }]}>阈值</Text>
          <Text style={[styles.alertTh]} />
        </View>
        {(alerts as Array<Record<string, unknown>>).map(a => (
          <View key={a.id as number} style={[styles.alertRow, { borderBottomColor: colors.borderLight }]}>
            <Text style={[styles.alertTd, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
              {a.symbol as string ?? '-'}
            </Text>
            <Text style={[styles.alertTd, { color: colors.textSecondary }]}>
              {a.alert_type as string}
            </Text>
            <Text style={[styles.alertTd, styles.alertTdRight, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
              {a.threshold as number}
            </Text>
            <TouchableOpacity onPress={() => delAlert(a.id as number)} style={styles.deleteBtn}>
              <Text style={{ color: '#f87171', fontSize: 11 }}>删除</Text>
            </TouchableOpacity>
          </View>
        ))}
      </View>
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 14 },
  section: { marginBottom: 24 },
  sectionTitle: { fontSize: 14, fontWeight: '600', marginBottom: 6 },
  sectionDesc: { fontSize: 12, lineHeight: 18 },
  fieldRow: { marginBottom: 10 },
  fieldLabel: { fontSize: 12, marginBottom: 4 },
  fieldInput: {
    borderWidth: 1,
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 7,
    fontSize: 13,
  },
  connectBtn: {
    backgroundColor: '#059669',
    paddingHorizontal: 16,
    paddingVertical: 9,
    borderRadius: 6,
    alignSelf: 'flex-start',
    marginTop: 4,
  },
  saveBtn: {
    backgroundColor: '#2563eb',
    paddingHorizontal: 20,
    paddingVertical: 9,
    borderRadius: 6,
    alignSelf: 'flex-start',
    marginBottom: 24,
  },
  alertForm: { gap: 8, marginBottom: 12 },
  alertInput: { borderWidth: 1, borderRadius: 6, paddingHorizontal: 10, paddingVertical: 6, fontSize: 13 },
  alertTypeRow: { flexDirection: 'row', gap: 4, flexWrap: 'wrap' },
  typeBtn: { paddingHorizontal: 10, paddingVertical: 5, borderRadius: 4 },
  addBtn: {
    backgroundColor: '#15803d',
    paddingHorizontal: 14,
    paddingVertical: 7,
    borderRadius: 6,
    alignSelf: 'flex-start',
  },
  alertHeader: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 8, paddingHorizontal: 6 },
  alertTh: { flex: 1, fontSize: 12 },
  alertThRight: { textAlign: 'right' },
  alertRow: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 10, paddingHorizontal: 6, alignItems: 'center' },
  alertTd: { flex: 1, fontSize: 13 },
  alertTdRight: { textAlign: 'right' },
  deleteBtn: { paddingHorizontal: 4 },
})
