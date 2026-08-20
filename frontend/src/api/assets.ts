import { get, post } from './client'

export interface AssetDefinition {
  dataset: string
  bar: string
  version: string
  table_name: string
  primary_time_column: string
  interval_seconds?: number | null
  expected_freshness_sec?: number | null
  retention_days: number
  enabled: boolean
}

export interface AssetState {
  earliest_ts?: string | null
  latest_ts?: string | null
  row_count: number
  expected_rows?: number | null
  missing_rows?: number | null
  duplicates?: number | null
  invalid_rows?: number | null
  quality_score?: number | null
  freshness_lag_sec?: number | null
  status: string
  checked_at?: string | null
  full_recount_at?: string | null
  last_check_at?: string | null
  detail?: Record<string, unknown> | null
}

export interface Asset {
  id: number
  exchange: string
  market: string
  inst_id: string
  dataset: string
  bar?: string | null
  state?: AssetState | null
}

export interface AssetTreeItem {
  inst_id: string
  datasets: Record<string, {
    asset_id: number
    dataset: string
    bar?: string
    status: string
    row_count: number
    quality_score?: number | null
  }>
}

export interface AssetInstrumentSummary {
  inst_id: string
  dataset_count: number
  row_count: number
  status: string
}

export const getDefinitions = () => get<AssetDefinition[]>('/api/assets/definitions')

export const getAssets = (params: { inst_id?: string; dataset?: string; status?: string; limit?: number } = {}) => {
  const q = new URLSearchParams()
  if (params.inst_id) q.set('inst_id', params.inst_id)
  if (params.dataset) q.set('dataset', params.dataset)
  if (params.status) q.set('status', params.status)
  q.set('limit', String(params.limit ?? 200))
  return get<{ total: number; items: Asset[] }>(`/api/assets?${q.toString()}`)
}

export const getAssetTree = () => get<AssetTreeItem[]>('/api/assets/tree')

export const getAssetInstruments = (params: { keyword?: string; status?: string; limit?: number; offset?: number } = {}) => {
  const q = new URLSearchParams()
  if (params.keyword) q.set('keyword', params.keyword)
  if (params.status) q.set('status', params.status)
  q.set('limit', String(params.limit ?? 100))
  q.set('offset', String(params.offset ?? 0))
  return get<{ total: number; items: AssetInstrumentSummary[] }>(`/api/assets/instruments?${q.toString()}`)
}

export const getAsset = (id: number) => get<Asset>(`/api/assets/${id}`)

export const refreshAssets = (scope: string, mode: string, inst_id?: string) =>
  post<Record<string, unknown>>('/api/assets/refresh', { scope, mode, inst_id })

export const refreshAsset = (id: number) =>
  post<Record<string, unknown>>(`/api/assets/${id}/refresh`)
