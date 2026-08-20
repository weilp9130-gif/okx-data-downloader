import { defineStore } from 'pinia'
import { ref } from 'vue'

import { listTasks, type TaskJob } from '@/api/tasks'
import { wsClient } from '@/api/ws'

export const useTasksStore = defineStore('tasks', () => {
  const items = ref<TaskJob[]>([])
  const total = ref(0)
  const loading = ref(false)
  const filter = ref<{ status?: string; task_type?: string }>({})

  const byId = new Map<string, TaskJob>()

  function applyJob(job: TaskJob): void {
    byId.set(job.id, job)
    const idx = items.value.findIndex((t) => t.id === job.id)
    if (idx >= 0) {
      items.value[idx] = job
    }
  }

  async function refresh(): Promise<void> {
    loading.value = true
    try {
      const res = await listTasks({ ...filter.value, limit: 100 })
      items.value = res.items
      total.value = res.total
      res.items.forEach((j) => byId.set(j.id, j))
    } finally {
      loading.value = false
    }
  }

  function upsertFromWs(id: string, status: string, progress?: Record<string, unknown> | null, attemptNo?: number): void {
    const job = byId.get(id)
    if (job) {
      job.status = status
      if (progress) job.progress = progress as TaskJob['progress']
      if (attemptNo !== undefined) job.attempt_no = attemptNo
    }
  }

  const unsub = wsClient.onMessage((msg) => {
    if (msg.type === 'job_update') {
      upsertFromWs(msg.data.id, msg.data.status, msg.data.progress, msg.data.attempt_no)
    }
  })
  wsClient.connect()

  return { items, total, loading, filter, refresh, applyJob, upsertFromWs, unsub }
})
