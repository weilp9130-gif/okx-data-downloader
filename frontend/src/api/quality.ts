import { get, post } from './client'

export interface QualityScore {
  inst_id: string
  dataset: string
  bar?: string | null
  quality_score: number
  status: string
  row_count: number
  last_check_at?: string | null
  detail?: Record<string, unknown> | null
}

export const runQualityCheck = (instId: string, bar = '1D', crossSource = false) =>
  post<Record<string, unknown>>('/api/quality/check', {
    inst_id: instId,
    bar,
    cross_source: crossSource,
  })

export const getQualityScores = (params: { inst?: string; bar?: string; dataset?: string; limit?: number } = {}) => {
  const q = new URLSearchParams()
  if (params.inst) q.set('inst', params.inst)
  if (params.bar) q.set('bar', params.bar)
  if (params.dataset) q.set('dataset', params.dataset)
  q.set('limit', String(params.limit ?? 100))
  return get<{ total: number; items: QualityScore[] }>(`/api/quality/score?${q.toString()}`)
}
