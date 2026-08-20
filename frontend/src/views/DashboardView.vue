<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'

import StatCard from '@/components/StatCard.vue'
import SysHealth from '@/components/SysHealth.vue'
import TaskTable from '@/components/TaskTable.vue'
import LogDrawer from '@/components/LogDrawer.vue'
import { stopTask, type TaskJob } from '@/api/tasks'
import { listTasks } from '@/api/tasks'
import { useSystemStore } from '@/stores/system'

const store = useSystemStore()
const recentTasks = ref<TaskJob[]>([])
const selected = ref<TaskJob | null>(null)
const drawer = ref(false)
let timer: number | null = null

function fmtBytes(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)} GB`
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`
  return `${(n / 1e3).toFixed(1)} KB`
}

async function refresh(): Promise<void> {
  await store.refreshDashboard()
  const res = await listTasks({ limit: 8 })
  recentTasks.value = res.items
}

async function onStop(job: TaskJob): Promise<void> {
  await stopTask(job.id)
  refresh()
}

function onView(job: TaskJob): void {
  selected.value = job
  drawer.value = true
}

const statusCounts = (): Record<string, number> => {
  const m: Record<string, number> = {}
  Object.values(store.dashboard?.collection_status ?? {}).forEach((s) => {
    m[s] = (m[s] ?? 0) + 1
  })
  return m
}

onMounted(async () => {
  await refresh()
  timer = window.setInterval(refresh, 15000)
})

onBeforeUnmount(() => {
  if (timer !== null) window.clearInterval(timer)
})
</script>

<template>
  <div class="page">
    <div class="cards">
      <StatCard title="资产数" :value="store.dashboard?.assets_count ?? 0" />
      <StatCard title="交易对" :value="store.dashboard?.inst_count ?? 0" />
      <StatCard title="数据总量(行)" :value="(store.dashboard?.total_rows ?? 0).toLocaleString()" />
      <StatCard title="存储占用" :value="fmtBytes(store.dashboard?.storage_bytes ?? 0)" />
      <StatCard title="运行中任务" :value="store.dashboard?.running_tasks ?? 0" tone="warning" />
      <StatCard title="异常资产" :value="store.dashboard?.abnormal_assets ?? 0" tone="danger" />
      <StatCard title="延迟 P50/P95/P99" :value="store.dashboard?.latency ? `${store.dashboard.latency.p50_ms}/${store.dashboard.latency.p95_ms}/${store.dashboard.latency.p99_ms}` : '-'" unit="ms" />
    </div>

    <div class="row">
      <div class="card block">
        <h3>系统健康</h3>
        <SysHealth v-if="store.dashboard" :health="store.dashboard.health" />
      </div>
      <div class="card block">
        <h3>采集状态</h3>
        <div class="status-list">
          <div v-for="(n, s) in statusCounts()" :key="s" class="status-item">
            <span class="status-dot" :class="s === 'HEALTHY' ? 'status-success' : s === 'WARNING' ? 'status-warning' : s === 'NO_DATA' ? 'status-muted' : 'status-danger'" />
            <span>{{ s }}</span>
            <span class="muted">{{ n }}</span>
          </div>
        </div>
      </div>
    </div>

    <div class="card block">
      <h3>最近任务</h3>
      <TaskTable :jobs="recentTasks" @view="onView" @stop="onStop" />
    </div>

    <LogDrawer v-model:visible="drawer" :job="selected" />
  </div>
</template>

<style scoped>
.cards {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}
.row {
  display: flex;
  gap: 16px;
  margin-bottom: 16px;
}
.block {
  flex: 1;
}
h3 {
  margin: 0 0 12px;
  font-size: 14px;
}
.status-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.status-item {
  display: flex;
  align-items: center;
  gap: 8px;
}
.status-item span:last-child {
  margin-left: auto;
}
</style>
