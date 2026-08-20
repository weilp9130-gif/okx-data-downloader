import { afterEach, describe, expect, it, vi } from 'vitest'

import { HttpError, request } from '../src/api/client'

describe('HTTP client', () => {
  afterEach(() => vi.restoreAllMocks())

  it('parses structured JSON errors without reading the response twice', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(
      JSON.stringify({ detail: '数据库不可用', code: 'DATABASE_UNAVAILABLE', retryable: true }),
      { status: 503, headers: { 'Content-Type': 'application/json' } },
    )))

    await expect(request('/api/dashboard')).rejects.toMatchObject({
      status: 503,
      detail: { code: 'DATABASE_UNAVAILABLE', retryable: true },
    } satisfies Partial<HttpError>)
  })

  it('keeps plain-text proxy errors readable', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('upstream unavailable', { status: 502 })))
    await expect(request('/api/health')).rejects.toMatchObject({
      status: 502,
      detail: 'upstream unavailable',
    } satisfies Partial<HttpError>)
  })
})
