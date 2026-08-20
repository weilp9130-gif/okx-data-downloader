<script setup lang="ts">
import type { TaskJob } from '@/api/tasks'

defineProps<{
  jobs: TaskJob[]
  loading?: boolean
}>()

const emit = defineEmits<{
  (e: 'view', job: TaskJob): void
  (e: 'stop', job: TaskJob): void
}>()

const statusTagType = (s: string): 'success' | 'warning' | 'danger' | 'info' => {
  if (s === 'SUCCESS') return 'success'
  if (s === 'RUNNING' || s === 'ASSIGNED' || s === 'PENDING') return 'warning'
  if (s === 'QUEUED' || s === 'PAUSED') return 'info'
  if (s === 'FAILED' || s === 'INTERRUPTED') return 'danger'
  return 'info'
}

const fmtTs = (v?: string | null): string => (v ? new Date(v).toLocaleString() : '-')
</script>

<template>
  <el-table :data="jobs" v-loading="loading" style="width: 100%" size="small">
    <el-table-column prop="task_no" label="任务号" width="150" />
    <el-table-column prop="task_type" label="类型" width="130" />
    <el-table-column label="状态" width="110">
      <template #default="{ row }">
        <el-tag :type="statusTagType(row.status)" size="small">{{ row.status }}</el-tag>
      </template>
    </el-table-column>
    <el-table-column label="进度" min-width="180">
      <template #default="{ row }">
        <el-progress
          v-if="row.progress && row.progress.percent !== undefined && row.progress.percent !== null"
          :percentage="Math.min(row.progress.percent, 100)"
          :show-text="false"
          :stroke-width="6"
        />
        <span v-else class="muted">
          {{ row.progress?.stage ?? '运行中' }}
        </span>
        <span v-if="row.progress?.written !== undefined" class="muted mono" style="font-size: 12px">
          {{ row.progress.written }}{{ row.progress.expected ? ' / ' + row.progress.expected : '' }}
          <template v-if="row.progress.rate"> @ {{ row.progress.rate }}/s</template>
          <template v-if="row.progress.eta_sec !== undefined && row.progress.eta_sec !== null">
            ETA {{ Math.ceil(row.progress.eta_sec / 60) }}m
          </template>
        </span>
      </template>
    </el-table-column>
    <el-table-column label="attempt" width="70" prop="attempt_no" />
    <el-table-column label="创建时间" min-width="150">
      <template #default="{ row }">{{ fmtTs(row.created_at) }}</template>
    </el-table-column>
    <el-table-column label="操作" width="160" fixed="right">
      <template #default="{ row }">
        <el-button size="small" text type="primary" @click="emit('view', row)">详情</el-button>
        <el-button
          v-if="['QUEUED', 'ASSIGNED', 'RUNNING', 'PAUSED'].includes(row.status)"
          size="small"
          text
          type="danger"
          @click="emit('stop', row)"
        >
          停止
        </el-button>
      </template>
    </el-table-column>
  </el-table>
</template>
