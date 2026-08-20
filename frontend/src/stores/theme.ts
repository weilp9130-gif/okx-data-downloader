import { defineStore } from 'pinia'
import { ref, watch } from 'vue'

export type ThemeMode = 'system' | 'light' | 'dark'

const KEY = 'okx-theme'

function resolve(mode: ThemeMode): 'light' | 'dark' {
  if (mode !== 'system') return mode
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export const useThemeStore = defineStore('theme', () => {
  const mode = ref<ThemeMode>((localStorage.getItem(KEY) as ThemeMode) || 'system')

  function apply(next = mode.value): void {
    document.documentElement.classList.toggle('dark', resolve(next) === 'dark')
  }

  function setMode(next: ThemeMode): void {
    mode.value = next
  }

  watch(mode, (next) => {
    localStorage.setItem(KEY, next)
    apply(next)
  }, { immediate: true })

  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (mode.value === 'system') apply()
  })

  return { mode, setMode, apply }
})
