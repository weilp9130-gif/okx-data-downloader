import { defineStore } from 'pinia'
import { ref } from 'vue'

import { getAssetInstruments, getDefinitions, type AssetDefinition, type AssetInstrumentSummary } from '@/api/assets'

export const useAssetsStore = defineStore('assets', () => {
  const instruments = ref<AssetInstrumentSummary[]>([])
  const instrumentTotal = ref(0)
  const definitions = ref<AssetDefinition[]>([])
  const loading = ref(false)

  async function refreshInstruments(keyword = '', status = ''): Promise<void> {
    loading.value = true
    try {
      const result = await getAssetInstruments({ keyword, status, limit: 500 })
      instruments.value = result.items
      instrumentTotal.value = result.total
    } finally {
      loading.value = false
    }
  }

  async function refreshDefinitions(): Promise<void> {
    definitions.value = await getDefinitions()
  }

  return { instruments, instrumentTotal, definitions, loading, refreshInstruments, refreshDefinitions }
})
