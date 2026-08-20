<script setup lang="ts">
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { init, type ECharts } from 'echarts/core'
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'

import type { LatencySummary } from '@/api/monitor'

use([LineChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer])

const props = defineProps<{ summaries: LatencySummary[] }>()

const el = ref<HTMLDivElement | null>(null)
let chart: ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function render(): void {
  if (!el.value || !chart) return
  const rows = props.summaries.filter((s) => s.metric === 'corrected_ws_receive_latency').slice(0, 100).reverse()
  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { textStyle: { color: '#94a3b8' } },
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    xAxis: {
      type: 'category',
      data: rows.map((r) => new Date(r.window_start).toLocaleTimeString()),
      axisLabel: { color: '#94a3b8' },
    },
    yAxis: {
      type: 'value',
      name: 'ms',
      nameTextStyle: { color: '#94a3b8' },
      axisLabel: { color: '#94a3b8' },
      splitLine: { lineStyle: { color: '#233047' } },
    },
    series: [
      { name: 'p50', type: 'line', smooth: true, data: rows.map((r) => r.p50_ms), itemStyle: { color: '#2563eb' } },
      { name: 'p95', type: 'line', smooth: true, data: rows.map((r) => r.p95_ms), itemStyle: { color: '#f59e0b' } },
      { name: 'p99', type: 'line', smooth: true, data: rows.map((r) => r.p99_ms), itemStyle: { color: '#ef4444' } },
    ],
  })
}

watch(() => props.summaries, render)

onMounted(() => {
  chart = init(el.value as HTMLDivElement)
  resizeObserver = new ResizeObserver(() => chart?.resize())
  resizeObserver.observe(el.value as HTMLDivElement)
  render()
})

onBeforeUnmount(() => {
  chart?.dispose()
  resizeObserver?.disconnect()
})
</script>

<template>
  <div ref="el" class="latency-chart" />
</template>

<style scoped>
.latency-chart {
  height: 260px;
  width: 100%;
}
</style>
