import { get, post } from './client'

export interface WorkerInfo {
  id: string
  name: string
  node?: string | null
  hostname?: string | null
  ip?: string | null
  python_version?: string | null
  os?: string | null
  worker_version?: string | null
  capabilities: string[]
  status: string
  capacity: number
  last_heartbeat_at?: string | null
  current_task_count: number
  last_error?: string | null
  online?: boolean
}

export interface LatencySummary {
  window_start: string
  source: string
  inst_id: string
  channel: string
  metric: string
  n: number
  p50_ms: number
  p95_ms: number
  p99_ms: number
  max_ms: number
  jitter_ms: number
}

export interface LatencySample {
  sample_ts: string
  session: number
  inst_id: string
  channel: string
  metric: string
  value_ms: number
}

export interface LatencyStat {
  window_start: string
  source: string
  metric: string
  value: number
}

export const getSystemHealth = () =>
  get<{ db: boolean; timescale: boolean; okx_rest: boolean | null; okx_ws: boolean | null; worker: boolean; status: string }>(
    '/api/monitor/system',
  )

export const getWorkers = () => get<{ items: WorkerInfo[] }>('/api/monitor/workers')

export const getLatencySummary = (params: { hours?: number; channel?: string; metric?: string } = {}) => {
  const q = new URLSearchParams()
  q.set('hours', String(params.hours ?? 24))
  if (params.channel) q.set('channel', params.channel)
  if (params.metric) q.set('metric', params.metric)
  return get<LatencySummary[]>(`/api/monitor/latency/summary?${q.toString()}`)
}

export const getLatencyLive = (params: { limit?: number } = {}) => {
  const q = new URLSearchParams()
  q.set('limit', String(params.limit ?? 200))
  return get<LatencySample[]>(`/api/monitor/latency/live?${q.toString()}`)
}

export const getLatencyStats = (params: { hours?: number } = {}) => {
  const q = new URLSearchParams()
  q.set('hours', String(params.hours ?? 24))
  return get<LatencyStat[]>(`/api/monitor/latency/stats?${q.toString()}`)
}

export const runLatencyProbe = (insts: string[], channels: string[], duration: number) =>
  post<Record<string, unknown>>('/api/tasks', {
    task_type: 'LATENCY_PROBE',
    params: { insts, channels, duration },
  })

export const runInstrumentsSync = () =>
  post<Record<string, unknown>>('/api/tasks', {
    task_type: 'INSTRUMENTS',
    params: { inst_type: 'SWAP' },
  })
