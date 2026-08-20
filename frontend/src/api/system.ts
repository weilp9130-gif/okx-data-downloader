import { get } from './client'

export interface SystemInfo {
  version: string
  python: string
  platform: string
  db_host: string
  db_name: string
  okx: Record<string, unknown>
  download: Record<string, unknown>
  masked_keys: Record<string, string>
  env: Record<string, string>
}

export const getSystemInfo = () => get<SystemInfo>('/api/system/info')

export const getLogFiles = () => get<{ files: Array<{ name: string; size: number; modified: number }> }>('/api/system/logs')

export const readLog = (file: string, offset = 0) =>
  get<{ content: string; offset: number; size: number; file: string }>(
    `/api/system/log?file=${encodeURIComponent(file)}&offset=${offset}`,
  )

export const getDashboard = () =>
  get<{
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
  }>('/api/dashboard')

export const getInstruments = (params: { inst_type?: string; keyword?: string; limit?: number } = {}) => {
  const q = new URLSearchParams()
  q.set('inst_type', params.inst_type ?? 'SWAP')
  if (params.keyword) q.set('keyword', params.keyword)
  q.set('limit', String(params.limit ?? 200))
  return get<{ source: string; total: number; items: string[] }>(`/api/instruments?${q.toString()}`)
}

export const getBars = () => get<{ items: string[] }>('/api/bars')

export const getHealth = () => get<{
  status: string
  db: boolean
  timescale: boolean
  okx_rest: boolean | null
  okx_ws: boolean | null
  worker: boolean
  db_error?: string | null
  updated_at?: string
}>('/api/health')
