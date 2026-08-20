<script setup lang="ts">
import { use, init, type ECharts } from 'echarts/core'
import { ScatterChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

import LatencyChart from '@/components/LatencyChart.vue'
import StatCard from '@/components/StatCard.vue'
import SysHealth from '@/components/SysHealth.vue'
import { runLatencyProbe } from '@/api/monitor'
import { getSystemHealth } from '@/api/monitor'
import { useLatencyStore } from '@/stores/latency'
import { useSystemStore } from '@/stores/system'

use([ScatterChart, GridComponent, TooltipComponent, CanvasRenderer])

const store = useLatencyStore()
const systemStore = useSystemStore()
const health = ref<{
  db: boolean
  timescale: boolean
  okx_rest: boolean | null
  okx_ws: boolean | null
  worker: boolean
  status: string
} | null>(null)

const probeInst = ref('BTC-USDT-SWAP')
const probeChannels = ref(['trades'])
const probeDuration = ref(60)
const startingProbe = ref(false)

const channelOptions = ['trades', 'bbo-tbt', 'books5', 'books', 'mark-price', 'index-tickers', 'tickers']

const liveChartEl = ref<HTMLDivElement | null>(null)
let liveChart: ECharts | null = null
let resizeObserver: ResizeObserver | null = null

let timer: number | null = null

function renderLive(): void {
  if (!liveChart || !liveChartEl.value) return
  const rows = store.live.slice(0, 100).reverse()
  liveChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: {
      type: 'category',
      data: rows.map((r) => new Date(r.sample_ts).toLocaleTimeString()),
      axisLabel: { color: '#94a3b8' },
    },
    yAxis: { type: 'value', name: 'ms', nameTextStyle: { color: '#94a3b8' }, axisLabel: { color: '#94a3b8' }, splitLine: { lineStyle: { color: '#233047' } } },
    series: [{ name: 'latency', type: 'scatter', data: rows.map((r) => r.value_ms), itemStyle: { color: '#2563eb' } }],
  })
}

const reconnectCount = () =>
  store.stats.filter((s) => s.metric === 'ws_reconnect').reduce((acc, s) => acc + s.value, 0)
const seqGapCount = () =>
  store.stats.filter((s) => s.metric === 'seq_gap').reduce((acc, s) => acc + s.value, 0)

async function refreshAll(): Promise<void> {
  await Promise.all([
    store.refreshSummaries(24),
    store.refreshLive(),
    store.refreshStats(24),
    store.refreshWorkers(),
  ])
  renderLive()
}

async function startProbe(): Promise<void> {
  startingProbe.value = true
  try {
    await runLatencyProbe(probeInst.value.split(',').map((s) => s.trim()), probeChannels.value, probeDuration.value)
    ElMessage.success('探针任务已创建')
  } finally {
    startingProbe.value = false
  }
}

onMounted(async () => {
  health.value = await getSystemHealth()
  liveChart = init(liveChartEl.value as HTMLDivElement)
  resizeObserver = new ResizeObserver(() => liveChart?.resize())
  resizeObserver.observe(liveChartEl.value as HTMLDivElement)
  await refreshAll()
  timer = window.setInterval(refreshAll, 5000)
})

onBeforeUnmount(() => {
  if (timer !== null) window.clearInterval(timer)
  liveChart?.dispose()
  resizeObserver?.disconnect()
})
</script>

<template>
  <div class="page">
    <div class="toolbar">
      <el-input v-model="probeInst" placeholder="交易对（逗号分隔）" style="width: 240px" />
      <el-select v-model="probeChannels" multiple style="width: 240px">
        <el-option v-for="c in channelOptions" :key="c" :value="c" :label="c" />
      </el-select>
      <el-input-number v-model="probeDuration" :min="30" :max="86400" />
      <el-button type="primary" :loading="startingProbe" @click="startProbe">启动探针</el-button>
    </div>

    <div class="cards">
      <StatCard title="重连次数" :value="reconnectCount()" />
      <StatCard title="序列缺口" :value="seqGapCount()" tone="warning" />
    </div>

    <div class="card block" style="margin-top: 12px">
      <h3>24h 延迟趋势（corrected WS receive）</h3>
      <LatencyChart :summaries="store.summaries" />
    </div>

    <div class="row">
      <div class="card block">
        <h3>实时延迟样本（5s 轮询）</h3>
        <div ref="liveChartEl" class="live-chart" />
      </div>
      <div class="card block">
        <h3>系统健康</h3>
        <SysHealth v-if="health" :health="health" />
        <h3 style="margin-top: 20px">Worker 列表</h3>
        <el-table :data="store.workers" size="small" max-height="300">
          <el-table-column prop="name" label="名称" width="90" />
          <el-table-column prop="hostname" label="主机" width="130" />
          <el-table-column prop="status" label="状态" width="80" />
          <el-table-column prop="python_version" label="Python" width="80" />
          <el-table-column label="能力" min-width="120">
            <template #default="{ row }">
              <el-tag v-for="c in row.capabilities" :key="c" size="small" style="margin-right: 4px">{{ c }}</el-tag>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </div>
  </div>
</template>

<style scoped>
.toolbar {
  display: flex;
  gap: 12px;
  margin-bottom: 12px;
  align-items: center;
}
.cards {
  display: flex;
  gap: 12px;
}
h3 {
  margin: 0 0 12px;
  font-size: 14px;
}
.row {
  display: flex;
  gap: 16px;
  margin-top: 16px;
}
.live-chart {
  height: 260px;
  width: 100%;
}
</style>
