<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

import { getQualityScores, runQualityCheck, type QualityScore } from '@/api/quality'

const scores = ref<QualityScore[]>([])
const instFilter = ref('')
const datasetFilter = ref('')
const running = ref(false)

async function refresh(): Promise<void> {
  scores.value = (await getQualityScores({ inst: instFilter.value || undefined, dataset: datasetFilter.value || undefined })).items
}

async function run(score: QualityScore): Promise<void> {
  running.value = true
  try {
    await runQualityCheck(score.inst_id, score.bar ?? '1D', false)
    ElMessage.success(`已提交 ${score.inst_id} 质量检查`)
  } finally {
    running.value = false
  }
}

async function runAll(): Promise<void> {
  running.value = true
  try {
    const insts = [...new Set(scores.value.map((s) => s.inst_id))].slice(0, 10)
    for (const inst of insts) {
      await runQualityCheck(inst, '1D', false)
    }
    ElMessage.success(`已提交 ${insts.length} 个质量检查`)
  } finally {
    running.value = false
  }
}

onMounted(refresh)
</script>

<template>
  <div class="page">
    <div class="toolbar">
      <el-input v-model="instFilter" placeholder="交易对过滤" style="width: 220px" clearable @change="refresh" />
      <el-input v-model="datasetFilter" placeholder="数据集过滤" style="width: 160px" clearable @change="refresh" />
      <el-button type="primary" text @click="refresh">刷新</el-button>
      <el-button :loading="running" @click="runAll">运行检查(前10)</el-button>
    </div>
    <div class="card">
      <el-table :data="scores" size="small" v-loading="running">
        <el-table-column prop="inst_id" label="交易对" width="170" />
        <el-table-column prop="dataset" label="数据集" width="140" />
        <el-table-column prop="bar" label="周期" width="80" />
        <el-table-column label="四维评分" min-width="200">
          <template #default="{ row }">
            <el-progress :percentage="row.quality_score" :stroke-width="10" :color="row.quality_score >= 90 ? '#10b981' : row.quality_score >= 80 ? '#f59e0b' : '#ef4444'">
              <span class="score-val">{{ row.quality_score }}</span>
            </el-progress>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'HEALTHY' ? 'success' : row.status === 'NO_DATA' ? 'info' : 'warning'" size="small">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="行数" prop="row_count" width="90" />
        <el-table-column label="最近检查" min-width="150">
          <template #default="{ row }">{{ row.last_check_at ? new Date(row.last_check_at).toLocaleString() : '-' }}</template>
        </el-table-column>
        <el-table-column label="操作" width="120">
          <template #default="{ row }">
            <el-button size="small" text type="primary" @click="run(row)">运行检查</el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<style scoped>
.toolbar {
  display: flex;
  gap: 12px;
  margin-bottom: 12px;
  align-items: center;
}
.score-val {
  font-size: 12px;
}
</style>
