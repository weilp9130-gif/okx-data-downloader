export interface WsJobUpdate {
  id: string
  task_no: string
  task_type: string
  inst?: string | null
  status: string
  progress?: Record<string, unknown> | null
  attempt_no: number
  group_id?: string | null
  error?: string | null
}

export interface WsWorkerUpdate {
  id: string
  name: string
  node?: string | null
  hostname?: string | null
  status: string
  last_heartbeat_at?: string | null
  current_task_count: number
  capabilities: string[]
}

type WsMessage =
  | { type: 'job_update'; data: WsJobUpdate }
  | { type: 'worker_update'; data: WsWorkerUpdate }
  | { type: 'ping'; data: { ts: number } }

type Handler = (msg: WsMessage) => void

const BACKOFF_MAX = 30000
const BACKOFF_BASE = 1000

export class WsClient {
  private ws: WebSocket | null = null
  private handlers: Handler[] = []
  private retry = 0
  private timer: number | null = null
  private stopped = false

  onMessage(handler: Handler): () => void {
    this.handlers.push(handler)
    return () => {
      this.handlers = this.handlers.filter((h) => h !== handler)
    }
  }

  connect(): void {
    this.stopped = false
    this.open()
  }

  private open(): void {
    if (this.stopped) return
    const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${proto}://${window.location.host}/ws`)
    this.ws = ws
    ws.onopen = () => {
      this.retry = 0
    }
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data) as WsMessage
        this.handlers.forEach((h) => h(msg))
      } catch {
        /* ignore malformed frames */
      }
    }
    ws.onclose = () => {
      this.scheduleReconnect()
    }
    ws.onerror = () => {
      ws.close()
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped) return
    const delay = Math.min(BACKOFF_BASE * 2 ** this.retry, BACKOFF_MAX)
    this.retry += 1
    if (this.timer !== null) window.clearTimeout(this.timer)
    this.timer = window.setTimeout(() => this.open(), delay)
  }

  close(): void {
    this.stopped = true
    if (this.timer !== null) window.clearTimeout(this.timer)
    this.ws?.close()
    this.ws = null
  }
}

export const wsClient = new WsClient()
