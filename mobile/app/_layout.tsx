import { useState, useEffect } from 'react'
import { View, Text, TouchableOpacity, StyleSheet, LogBox, Modal, FlatList, Pressable } from 'react-native'
import { useSafeAreaInsets } from 'react-native-safe-area-context'
import { Stack, useRouter, usePathname } from 'expo-router'

LogBox.ignoreLogs(['Failed to fetch symbols'])
import { WebSocketProvider } from '../src/components/WebSocketProvider'
import { useMarketStore } from '../src/stores/marketStore'
import { ThemeProvider, useTheme } from '../src/theme'
import { getSymbolDescription } from '../src/config/productConfig'
import { api } from '../src/api/client'

function ConnectionDot() {
  const connected = useMarketStore(s => s.connected)
  return (
    <View style={[styles.dot, { backgroundColor: connected ? '#4ade80' : '#f87171' }]} />
  )
}

function ThemeToggleButton() {
  const { theme, toggleTheme, colors } = useTheme()
  return (
    <TouchableOpacity onPress={toggleTheme} style={[styles.iconBtn, { backgroundColor: colors.raised, borderColor: colors.border }]}>
      <Text style={{ fontSize: 16, color: colors.textSecondary }}>
        {theme === 'dark' ? '\u2600\uFE0F' : '\u{1F319}'}
      </Text>
    </TouchableOpacity>
  )
}

export default function RootLayout() {
  return (
    <ThemeProvider>
      <AppLayout />
    </ThemeProvider>
  )
}

function AppLayout() {
  const insets = useSafeAreaInsets()
  const { colors } = useTheme()
  const router = useRouter()
  const pathname = usePathname()
  const [showSymbolPicker, setShowSymbolPicker] = useState(false)

  const activeSymbol = useMarketStore(s => s.activeSymbol)
  const setActiveSymbol = useMarketStore(s => s.setActiveSymbol)
  const quotes = useMarketStore(s => s.quotes)
  const initQuotes = useMarketStore(s => s.initQuotes)
  const symbols = Object.keys(quotes)

  useEffect(() => {
    api.get<any[]>('/symbols').then(data => {
      if (Array.isArray(data)) {
        const symList = data.map((s: any) => s.symbol)
        initQuotes(symList)
        if (symList.length > 0 && !useMarketStore.getState().activeSymbol) {
          useMarketStore.getState().setActiveSymbol(symList[0])
        }
      }
    }).catch(err => console.error('Failed to fetch symbols:', err))
  }, [initQuotes])

  return (
    <View style={[styles.root, { backgroundColor: colors.background, paddingTop: insets.top }]}>
      <WebSocketProvider />

      {/* Top Navigation Bar */}
      <View style={[styles.topBar, { backgroundColor: colors.surface, borderBottomColor: colors.border }]}>
        <ConnectionDot />

        {/* Symbol Selector Dropdown */}
        <TouchableOpacity
          style={[styles.symbolPicker, { backgroundColor: colors.elevated, borderColor: colors.border }]}
          onPress={() => setShowSymbolPicker(true)}
        >
          <Text style={[styles.symbolText, { color: colors.textPrimary }]}>
            {activeSymbol || '--'}
          </Text>
          <Text style={[styles.symbolDesc, { color: colors.textSecondary }]}>
            {activeSymbol ? getSymbolDescription(activeSymbol) : ''}
          </Text>
          <Text style={{ fontSize: 10, marginLeft: 4, color: colors.textMuted }}>{'\u25BC'}</Text>
        </TouchableOpacity>

        {/* Symbol Picker Modal */}
        <Modal visible={showSymbolPicker} transparent animationType="fade" onRequestClose={() => setShowSymbolPicker(false)}>
          <Pressable style={styles.modalOverlay} onPress={() => setShowSymbolPicker(false)}>
            <View style={[styles.modalContent, { backgroundColor: colors.surface, borderColor: colors.border }]}>
              <Text style={[styles.modalTitle, { color: colors.textHeading }]}>选择标的</Text>
              <FlatList
                data={symbols}
                keyExtractor={(item) => item}
                renderItem={({ item }) => (
                  <TouchableOpacity
                    style={[
                      styles.modalItem,
                      { backgroundColor: item === activeSymbol ? colors.elevated : 'transparent' },
                    ]}
                    onPress={() => {
                      setActiveSymbol(item)
                      setShowSymbolPicker(false)
                    }}
                  >
                    <Text style={[styles.modalItemSymbol, { color: colors.textPrimary }]}>{item}</Text>
                    <Text style={[styles.modalItemDesc, { color: colors.textSecondary }]}>
                      {getSymbolDescription(item)}
                    </Text>
                    {item === activeSymbol && <Text style={{ color: '#2563eb', fontSize: 14 }}>{'\u2713'}</Text>}
                  </TouchableOpacity>
                )}
                ItemSeparatorComponent={() => <View style={[styles.separator, { backgroundColor: colors.border }]} />}
              />
            </View>
          </Pressable>
        </Modal>

        <View style={styles.topActions}>
          <ThemeToggleButton />
        </View>
      </View>

      {/* Page Content */}
      <View style={styles.content}>
        <Stack
          screenOptions={{
            headerShown: false,
            animation: 'slide_from_right',
            contentStyle: { backgroundColor: colors.background },
          }}
        >
          <Stack.Screen name="index" />
          <Stack.Screen name="account" />
          <Stack.Screen name="orders" />
          <Stack.Screen name="history" />
          <Stack.Screen name="settings" />
        </Stack>
      </View>

      {/* Bottom Navigation */}
      <View style={[styles.bottomNav, { backgroundColor: colors.surface, borderTopColor: colors.border }]}>
        {navItems.map(item => {
          const isActive = pathname === item.route
          return (
            <TouchableOpacity
              key={item.route}
              onPress={() => router.push(item.route)}
              style={styles.navItem}
            >
              <Text style={[styles.navIcon]}>{item.icon}</Text>
              <Text
                style={[
                  styles.navLabel,
                  { color: isActive ? '#2563eb' : colors.textSecondary },
                ]}
              >
                {item.label}
              </Text>
            </TouchableOpacity>
          )
        })}
      </View>
    </View>
  )
}

const navItems = [
  { route: '/', label: '监控', icon: '\uD83D\uDCCA' },
  { route: '/account', label: '账户', icon: '\uD83D\uDCB0' },
  { route: '/orders', label: '订单', icon: '\uD83D\uDCCB' },
  { route: '/history', label: '历史', icon: '\uD83D\uDCC4' },
  { route: '/settings', label: '设置', icon: '\u2699\uFE0F' },
]

const styles = StyleSheet.create({
  root: {
    flex: 1,
  },
  topBar: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 8,
    paddingVertical: 6,
    borderBottomWidth: 1,
    gap: 8,
  },
  dot: {
    width: 10,
    height: 10,
    borderRadius: 5,
  },
  symbolPicker: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    height: 34,
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 10,
  },
  symbolText: {
    fontFamily: 'monospace',
    fontSize: 13,
    fontWeight: '600',
  },
  symbolDesc: {
    fontSize: 11,
    marginLeft: 6,
  },
  topActions: {
    flexDirection: 'row',
    gap: 6,
  },
  iconBtn: {
    width: 34,
    height: 34,
    borderRadius: 8,
    alignItems: 'center',
    justifyContent: 'center',
    borderWidth: 1,
  },
  content: {
    flex: 1,
  },
  bottomNav: {
    flexDirection: 'row',
    borderTopWidth: 1,
    paddingBottom: 4,
    paddingTop: 4,
  },
  navItem: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: 4,
  },
  navIcon: {
    fontSize: 18,
    marginBottom: 1,
  },
  navLabel: {
    fontSize: 10,
  },
  modalOverlay: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: 'rgba(0,0,0,0.5)',
  },
  modalContent: {
    width: '80%',
    maxHeight: '60%',
    borderRadius: 12,
    borderWidth: 1,
    overflow: 'hidden',
  },
  modalTitle: {
    fontSize: 15,
    fontWeight: '600',
    paddingHorizontal: 16,
    paddingVertical: 12,
  },
  modalItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
  },
  modalItemSymbol: {
    fontSize: 14,
    fontFamily: 'monospace',
    fontWeight: '600',
    width: 70,
  },
  modalItemDesc: {
    fontSize: 12,
    flex: 1,
  },
  separator: {
    height: 1,
  },
})
