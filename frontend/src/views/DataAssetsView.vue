<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { getAssets, refreshAsset, type Asset } from '@/api/assets'
import { useAssetsStore } from '@/stores/assets'

const store = useAssetsStore()
const keyword = ref('')
const selectedInst = ref('')
const assets = ref<Asset[]>([])
const selected = ref<Asset | null>(null)
const loadingAssets = ref(false)

async function refreshInstruments(): Promise<void> {
  await store.refreshInstruments(keyword.value)
}

async function selectInstrument(instId: string): Promise<void> {
  selectedInst.value = instId
  selected.value = null
  loadingAssets.value = true
  try {
    assets.value = (await getAssets({ inst_id: instId, limit: 1000 })).items
  } finally {
    loadingAssets.value = false
  }
}

async function onRefresh(): Promise<void> {
  if (!selected.value) return
  await refreshAsset(selected.value.id)
  ElMessage.success('资产状态已刷新')
  await selectInstrument(selected.value.inst_id)
}

onMounted(async () => {
  await Promise.all([refreshInstruments(), store.refreshDefinitions()])
})
</script>

<template>
  <div class="page assets-page">
    <div class="card instruments-card">
      <div class="toolbar">
        <h3>交易对（{{ store.instrumentTotal }}）</h3>
        <el-button size="small" text type="primary" @click="refreshInstruments">刷新</el-button>
      </div>
      <el-input v-model="keyword" clearable placeholder="搜索交易对" @change="refreshInstruments" />
      <el-scrollbar height="560px" v-loading="store.loading">
        <button v-for="item in store.instruments" :key="item.inst_id" class="instrument-row" :class="{ active: item.inst_id === selectedInst }" @click="selectInstrument(item.inst_id)">
          <span>{{ item.inst_id }}</span>
          <el-tag size="small" :type="item.status === 'HEALTHY' ? 'success' : item.status === 'NO_DATA' ? 'info' : 'warning'">{{ item.status }}</el-tag>
        </button>
      </el-scrollbar>
    </div>
    <div class="card datasets-card">
      <template v-if="selectedInst">
        <div class="toolbar"><h3>{{ selectedInst }} 数据资产</h3><span class="muted">{{ assets.length }} 项</span></div>
        <el-table :data="assets" size="small" v-loading="loadingAssets" max-height="590" @row-click="(row: Asset) => selected = row">
          <el-table-column prop="dataset" label="数据集" width="150" />
          <el-table-column prop="bar" label="周期" width="80" />
          <el-table-column label="状态" width="100"><template #default="{ row }"><el-tag size="small">{{ row.state?.status ?? 'NO_DATA' }}</el-tag></template></el-table-column>
          <el-table-column label="行数" width="120"><template #default="{ row }">{{ (row.state?.row_count ?? 0).toLocaleString() }}</template></el-table-column>
          <el-table-column label="质量分" width="90"><template #default="{ row }">{{ row.state?.quality_score ?? '-' }}</template></el-table-column>
          <el-table-column label="最新时间" min-width="180"><template #default="{ row }">{{ row.state?.latest_ts ?? '-' }}</template></el-table-column>
        </el-table>
      </template>
      <el-empty v-else description="选择左侧交易对查看资产" />
    </div>
    <div class="card detail-card">
      <template v-if="selected">
        <h3>{{ selected.dataset }}<template v-if="selected.bar"> / {{ selected.bar }}</template></h3>
        <el-descriptions :column="1" size="small" border>
          <el-descriptions-item label="状态">{{ selected.state?.status ?? 'NO_DATA' }}</el-descriptions-item>
          <el-descriptions-item label="行数">{{ selected.state?.row_count ?? 0 }}</el-descriptions-item>
          <el-descriptions-item label="最新时间">{{ selected.state?.latest_ts ?? '-' }}</el-descriptions-item>
          <el-descriptions-item label="新鲜度滞后">{{ selected.state?.freshness_lag_sec ?? '-' }} s</el-descriptions-item>
        </el-descriptions>
        <el-button size="small" type="primary" style="margin-top:12px" @click="onRefresh">刷新状态</el-button>
      </template>
      <el-empty v-else description="选择资产查看详情" />
    </div>
  </div>
</template>

<style scoped>
.assets-page { display: grid; grid-template-columns: 280px minmax(400px, 1fr) 240px; gap: 16px; }
h3 { margin: 0; font-size: 14px; }
.instrument-row { width: 100%; display: flex; justify-content: space-between; align-items: center; border: 0; border-bottom: 1px solid var(--border); padding: 10px 4px; background: transparent; color: var(--text-main); cursor: pointer; text-align: left; }
.instrument-row:hover, .instrument-row.active { background: var(--bg-hover); }
.datasets-card, .detail-card { min-width: 0; }
</style>
