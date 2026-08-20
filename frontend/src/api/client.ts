export interface ApiError {
  status: number
  detail: unknown
}

export class HttpError extends Error {
  status: number
  detail: unknown

  constructor(status: number, detail: unknown) {
    super(`HTTP ${status}: ${JSON.stringify(detail)}`)
    this.status = status
    this.detail = detail
  }
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    const raw = await res.text()
    let detail: unknown = raw
    try {
      detail = raw ? JSON.parse(raw) : { detail: res.statusText }
    } catch {
      // A proxy or a web server may return a non-JSON error page.
    }
    throw new HttpError(res.status, detail)
  }
  return (await res.json()) as T
}

export const get = <T>(path: string) => request<T>(path)
export const post = <T>(path: string, body?: unknown) =>
  request<T>(path, { method: 'POST', body: body === undefined ? undefined : JSON.stringify(body) })
