import { beforeEach, describe, expect, it, vi } from 'vitest'
import { nextTick } from 'vue'
import { createPinia, setActivePinia } from 'pinia'

const storage = new Map<string, string>()
const classes = new Set<string>()

beforeEach(() => {
  storage.clear()
  classes.clear()
  vi.resetModules()
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => storage.set(key, value),
  })
  vi.stubGlobal('document', { documentElement: { classList: {
    toggle: (name: string, enabled: boolean) => enabled ? classes.add(name) : classes.delete(name),
  } } })
  vi.stubGlobal('window', {
    matchMedia: () => ({ matches: true, addEventListener: vi.fn() }),
  })
  setActivePinia(createPinia())
})

describe('theme store', () => {
  it('persists the selected mode and applies the dark class', async () => {
    const { useThemeStore } = await import('../src/stores/theme')
    const store = useThemeStore()
    store.setMode('dark')
    await nextTick()
    expect(storage.get('okx-theme')).toBe('dark')
    expect(classes.has('dark')).toBe(true)
  })

  it('follows the system preference by default', async () => {
    const { useThemeStore } = await import('../src/stores/theme')
    useThemeStore()
    expect(classes.has('dark')).toBe(true)
    expect(storage.get('okx-theme')).toBe('system')
  })
})
