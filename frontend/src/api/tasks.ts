import { get, post } from './client'

export interface TaskJob {
  id: string
  group_id: string | null
  task_no: string
  task_type: string
  params: Record<string, unknown>
  status: string
  priority: number
  required_capability?: string | null
  rate_group?: string | null
  attempt_no: number
  retry_count: number
  max_retry: number
  progress?: {
    type?: string
    percent?: number | null
    written?: number
    expected?: number
    rate?: number | null
    eta_sec?: number | null
    stage?: string
  } | null
  error?: string | null
  created_at?: string
  started_at?: string | null
  finished_at?: string | null
  attempts?: TaskAttempt[]
}

export interface TaskAttempt {
  id: number
  attempt_no: number
  worker_id?: string | null
  pid?: number | null
  started_at: string
  finished_at?: string | null
  exit_code?: number | null
  log_path?: string | null
  progress_path?: string | null
  error?: string | null
}

export interface TaskList {
  total: number
  items: TaskJob[]
}

export interface TaskLog {
  content: string
  offset: number
  size: number
  attempt_no?: number
}

export const submitTask = (taskType: string, params: Record<string, unknown>) =>
  post<TaskJob>('/api/tasks', { task_type: taskType, params })

export const submitBatch = (taskType: string, paramsList: Record<string, unknown>[]) =>
  post<{ group_id: string; task_ids: string[] }>('/api/tasks/batch', {
    task_type: taskType,
    params_list: paramsList,
  })

export const listTasks = (params: {
  status?: string
  task_type?: string
  group_id?: string
  limit?: number
  offset?: number
} = {}) => {
  const q = new URLSearchParams()
  if (params.status) q.set('status', params.status)
  if (params.task_type) q.set('task_type', params.task_type)
  if (params.group_id) q.set('group_id', params.group_id)
  q.set('limit', String(params.limit ?? 100))
  q.set('offset', String(params.offset ?? 0))
  return get<TaskList>(`/api/tasks?${q.toString()}`)
}

export const getTask = (id: string) => get<TaskJob>(`/api/tasks/${id}`)

export const stopTask = (id: string) => post<TaskJob>(`/api/tasks/${id}/stop`)

export const pauseTask = (id: string) => post<TaskJob>(`/api/tasks/${id}/pause`)

export const resumeTask = (id: string) => post<TaskJob>(`/api/tasks/${id}/resume`)

export const getTaskLog = (id: string, offset = 0, attempt?: number) =>
  get<TaskLog>(`/api/tasks/${id}/log?offset=${offset}${attempt !== undefined ? `&attempt=${attempt}` : ''}`)

export const getTaskAttempts = (id: string) => get<TaskAttempt[]>(`/api/tasks/${id}/attempts`)
