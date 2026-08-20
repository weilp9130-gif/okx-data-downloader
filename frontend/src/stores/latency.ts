import { defineStore } from 'pinia'
import { ref } from 'vue'

import { getLatencyLive, getLatencyStats, getLatencySummary, getWorkers, type LatencySample, type LatencyStat, type LatencySummary, type WorkerInfo } from '@/api/monitor'

export const useLatencyStore = defineStore('latency', () => {
  const summaries = ref<LatencySummary[]>([])
  const live = ref<LatencySample[]>([])
  const stats = ref<LatencyStat[]>([])
  const workers = ref<WorkerInfo[]>([])

  async function refreshSummaries(hours = 24): Promise<void> {
    summaries.value = await getLatencySummary({ hours })
  }

  async function refreshLive(): Promise<void> {
    live.value = await getLatencyLive({ limit: 200 })
  }

  async function refreshStats(hours = 24): Promise<void> {
    stats.value = await getLatencyStats({ hours })
  }

  async function refreshWorkers(): Promise<void> {
    const res = await getWorkers()
    workers.value = res.items
  }

  return { summaries, live, stats, workers, refreshSummaries, refreshLive, refreshStats, refreshWorkers }
})
