import { defineStore } from 'pinia'
import { ref } from 'vue'

import { getDashboard, getHealth, getSystemInfo, type SystemInfo } from '@/api/system'

export interface DashboardData {
  assets_count: number
  inst_count: number
  total_rows: number
  storage_bytes: number
  running_tasks: number
  abnormal_assets: number
  latest_candle_ts?: string | null
  latency?: { p50_ms: number; p95_ms: number; p99_ms: number } | null
  health: { db: boolean; timescale: boolean; okx_rest: boolean | null; okx_ws: boolean | null; worker: boolean }
  collection_status: Record<string, string>
}

export const useSystemStore = defineStore('system', () => {
  const info = ref<SystemInfo | null>(null)
  const dashboard = ref<DashboardData | null>(null)
  const health = ref<Awaited<ReturnType<typeof getHealth>> | null>(null)
  const now = ref(new Date())

  async function refreshInfo(): Promise<void> {
    info.value = await getSystemInfo()
  }

  async function refreshDashboard(): Promise<void> {
    dashboard.value = await getDashboard()
  }

  async function refreshHealth(): Promise<void> {
    health.value = await getHealth()
  }

  function tick(): void {
    now.value = new Date()
  }

  return { info, dashboard, health, now, refreshInfo, refreshDashboard, refreshHealth, tick }
})
