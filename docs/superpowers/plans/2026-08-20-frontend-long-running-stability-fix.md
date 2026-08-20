# 前端长时间运行崩溃与内存泄漏修复方案 (Implementation Plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 彻底解决前端页面（Web 及移动端 WebView）长时间运行后因高频 WebSocket 推送、数据无限增长、死循环 RAF 轮询和定时器泄漏导致的卡死与 OOM 崩溃问题。

**Architecture:** 
1. 在 WebSocket 数据入口与 Zustand store 间建立节流缓冲层，阻断高频 Tick 引起的 React 组件树渲染洪泛；
2. 为图表历史数据及指标数组引入滑动窗口上限（Max Buffer Sliding Window），并将 KDJ 指标检索优化为 $O(1)$ Hash 映射；
3. 将主副图（K线与KDJ）同步从每帧无条件 `requestAnimationFrame` 轮询重构为 Lightweight Charts 原生事件驱动机制；
4. 规范所有页面级异步定时器、全局缓存 LRU 淘汰以及 store 订阅隔离。

**Tech Stack:** React 19, TypeScript, Lightweight Charts 5.1, Zustand 5.0, Vite 8, React Native WebView.

---

## Global Constraints
- 严格遵循最小侵入（Surgical Changes）原则，保留原有业务逻辑、K线算法与指标对齐行为。
- 不引入重型外部库，保持代码简洁轻量。
- 确保 Web 端与 Mobile 端保持一致的图表行为与交互体验。

---

## Tasks

### Task 1: 高频 Tick 数据节流缓冲与 Store 订阅隔离

**Files:**
- Modify: `frontend/src/components/WebSocketProvider.tsx`
- Modify: `frontend/src/pages/Monitor.tsx:30-45, 280-315`
- Modify: `frontend/src/pages/Account.tsx:50-80`

**Interfaces:**
- `useMarketStore.updateTick(t: Tick)`: 保持接口兼容，但在推送端施加 100ms 节流或仅在价格发生实质变化/时间跨度时触发 React 状态更新。
- `Account.tsx`: 将 `useMarketStore(s => s.quotes)` 解耦，改为使用 `useMarketStore.getState().quotes` 在 3 秒局部定时器中读取，避免任意行情推送引起整页 Re-render。

- [ ] **Step 1: 在 `WebSocketProvider.tsx` 中为 `/ws/tick` 增加节流合并**
  
```ts
// frontend/src/components/WebSocketProvider.tsx
// 对高频 tick 进行 100ms 节流合批更新，避免每秒数十次触发 Zustand 状态订阅
let lastTickTime = 0
let pendingTick: any = null
let tickTimer: ReturnType<typeof setTimeout> | null = null

const flushTick = () => {
  if (pendingTick) {
    updateTick(pendingTick)
    pendingTick = null
    lastTickTime = Date.now()
  }
  tickTimer = null
}

useWebSocket('/ws/tick', (data: unknown) => {
  if (isRecord(data) && typeof data.symbol === 'string') {
    const now = Date.now()
    pendingTick = data
    if (now - lastTickTime >= 100) {
      if (tickTimer) clearTimeout(tickTimer)
      flushTick()
    } else if (!tickTimer) {
      tickTimer = setTimeout(flushTick, 100 - (now - lastTickTime))
    }
  }
})
```

- [ ] **Step 2: 在 `Account.tsx` 中移除无意义的高频 `quotes` 订阅**

```ts
// frontend/src/pages/Account.tsx
// 移除: const quotes = useMarketStore(s => s.quotes)
// 改为在 getQuote 或定时器中通过 useMarketStore.getState().quotes 按需读取
function getQuote(sym: string) {
  return (useMarketStore.getState().quotes as Record<string, any>)?.[sym]
}
```

- [ ] **Step 3: 检查 `Monitor.tsx` 中 `chartLiveTick` 依赖与定时器清理**

```ts
// frontend/src/pages/Monitor.tsx
// 在 useEffect([activeSymbol, chartInterval, chartLiveTick...]) 中添加清理函数:
return () => {
  for (const timer of recentRefreshTimersRef.current) {
    window.clearTimeout(timer)
  }
  recentRefreshTimersRef.current = []
}
```

- [ ] **Step 4: 运行类型检查与构建验证**
Run: `cd frontend && npm run build`
Expected: 成功，无 TypeScript 错误。

---

### Task 2: 图表数据滑动窗口容量上限（Sliding Window）与 $O(1)$ KDJ 索引

**Files:**
- Modify: `frontend/src/components/CandleChart.tsx:470-530, 850-975`
- Modify: `mobile/assets/chart.html`

**Interfaces:**
- `MAX_CANDLE_BUFFER = 3000`: 单一图表实例最大保留历史柱数。当新增实时柱超出该上限时，自动丢弃最旧的柱。
- `kdjMapRef`: `Map<number, { k: number, d: number, j: number }>` 用于十字光标在移动时实现 $O(1)$ 查找，避免每帧对大数组执行线性 `.find()`。

- [ ] **Step 1: 在 `CandleChart.tsx` 中为实时追加数据设置容量上限**

```ts
// frontend/src/components/CandleChart.tsx
const MAX_CHART_BARS = 3000

// 在 isNewCandle 分支中：
if (currentData.length >= MAX_CHART_BARS) {
  currentData.shift() // 剔除最旧的一根柱，保持内存恒定
}
currentData.push(newCandle)
```

- [ ] **Step 2: 优化 KDJ 数据结构，引入 Map 索引支持 $O(1)$ 查找**

```ts
// 维护 kdjMap: Map<number, { k: number; d: number; j: number }>
// 十字光标悬停与副图同步查找时:
const kdjPoint = kdjMapRef.current.get(timeSec)
const kVal = kdjPoint?.k
const dVal = kdjPoint?.d
const jVal = kdjPoint?.j
// 替换原有的: kdjDataRef.current.k.find(x => x.time === timeSec)
```

- [ ] **Step 3: 同步在 `mobile/assets/chart.html` 中应用相同的滑动窗口与 Map 查找保护**

- [ ] **Step 4: 运行前端构建验证**
Run: `cd frontend && npm run build`
Expected: PASS

---

### Task 3: 移除无条件 RAF 死循环，改为事件驱动的主副图同步

**Files:**
- Modify: `frontend/src/components/CandleChart.tsx:420-475, 645-670`
- Modify: `mobile/assets/chart.html:590-635`

**Interfaces:**
- 主图与 KDJ 副图基于 `chart.timeScale().subscribeVisibleLogicalRangeChange` 进行双向/单向视野同步，仅在视野发生变动（用户拖拽、缩放、新柱进入视野）时触发同步逻辑，不占用空闲帧。

- [ ] **Step 1: 在 `CandleChart.tsx` 中用订阅事件替换 `requestAnimationFrame(syncLoop)`**

```ts
// 替换原有的 syncLoop RAF:
let syncing = false
const syncKdjRange = (mr: { from: number; to: number } | null) => {
  if (!mr || !kdjChartRef.current || syncing) return
  syncing = true
  try {
    const kdjK = kdjDataRef.current.k
    if (kdjK.length > 0) {
      const currentKdjK0Time = kdjK[0].time
      const mainData = lastDataRef.current
      if (mainData.length > 0) {
        const offset = mainData.findIndex(x => x.time === currentKdjK0Time)
        if (offset !== -1) {
          const from = mr.from - offset
          const to = mr.to - offset
          const kdjTimeScale = kdjChartRef.current.timeScale()
          const currentKdjRange = kdjTimeScale.getVisibleLogicalRange()
          if (!currentKdjRange || Math.abs(currentKdjRange.from - from) > 0.01 || Math.abs(currentKdjRange.to - to) > 0.01) {
            kdjTimeScale.setVisibleLogicalRange({ from, to })
          }
        }
      }
    }
  } catch {}
  syncing = false
}

// 绑定主图范围变动事件
const rangeChangeHandler = (range: any) => {
  if (range) syncKdjRange(range)
}
chart.timeScale().subscribeVisibleLogicalRangeChange(rangeChangeHandler)

// 在 cleanup 函数中移除订阅:
chart.timeScale().unsubscribeVisibleLogicalRangeChange(rangeChangeHandler)
```

- [ ] **Step 2: 在 `mobile/assets/chart.html` 中同步移除 `syncLoop` 的 RAF 轮询，改为事件监听**

- [ ] **Step 3: 运行前端构建验证**
Run: `cd frontend && npm run build`
Expected: PASS

---

### Task 4: 历史数据全局缓存 LRU 淘汰与 DOM 事件清理

**Files:**
- Modify: `frontend/src/pages/Monitor.tsx:10-25, 110-180`
- Modify: `frontend/src/components/CandleChart.tsx:380-425, 660-670`

**Interfaces:**
- `LRUMap<K, V>`: 限制最大 20 个周期的历史缓存条目，超出时自动 `delete(oldestKey)`，防止跨标的切换后内存常驻泄漏。
- `CandleChart.tsx`: 妥善注销 KDJ overlay 上的 pointer/touch 事件，并避免全局 `(window as any).__chartRanges` 无限制增加。

- [ ] **Step 1: 实现简易 LRU Map 替换 `Monitor.tsx` 中的全局无界 `historyCache`**

```ts
// frontend/src/pages/Monitor.tsx
class SimpleLRU<K, V> {
  private max: number
  private map: Map<K, V>
  constructor(max = 20) {
    this.max = max
    this.map = new Map()
  }
  get(key: K): V | undefined {
    const val = this.map.get(key)
    if (val !== undefined) {
      this.map.delete(key)
      this.map.set(key, val)
    }
    return val
  }
  set(key: K, val: V): void {
    if (this.map.has(key)) {
      this.map.delete(key)
    } else if (this.map.size >= this.max) {
      const oldestKey = this.map.keys().next().value
      if (oldestKey !== undefined) this.map.delete(oldestKey)
    }
    this.map.set(key, val)
  }
}
const historyCache = new SimpleLRU<string, CandleLike[]>(20)
```

- [ ] **Step 2: 规范 `CandleChart.tsx` 中 Overlay 事件监听与清理**

```ts
// 显式保留 listener 引用并在 return cleanup 中 removeEventListener
return () => {
  kdjOverlay.removeEventListener('pointerdown', onOverlayDown)
  kdjOverlay.removeEventListener('pointermove', onOverlayMove)
  kdjOverlay.removeEventListener('pointerup', onOverlayEnd)
  kdjOverlay.removeEventListener('pointerleave', onOverlayEnd)
  if (oldOverlay && oldOverlay.parentNode) {
    oldOverlay.parentNode.removeChild(oldOverlay)
  }
}
```

- [ ] **Step 3: 运行前端构建验证**
Run: `cd frontend && npm run build`
Expected: PASS

---

### Task 5: 整体回归验证与构建测试

- [ ] **Step 1: 运行全量前端构建与 Lint**
Run: `cd frontend && npm run build && npm run lint`
Expected: 构建成功且 Lint 检查无报错。

- [ ] **Step 2: 模拟长时间高频推送验证稳定性**
验证在快速切换周期、高频收到 Tick 推送、以及静止状态下主线程 CPU 占用归零、内存维持在稳定水位。
