<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import LogDrawer from '@/components/LogDrawer.vue'
import TaskTable from '@/components/TaskTable.vue'
import { pauseTask, resumeTask, stopTask, type TaskJob } from '@/api/tasks'
import { useTasksStore } from '@/stores/tasks'

const store = useTasksStore()
const statusFilter = ref('')
const drawer = ref(false)
const selected = ref<TaskJob | null>(null)
const statuses = ['QUEUED', 'ASSIGNED', 'RUNNING', 'PAUSED', 'SUCCESS', 'FAILED', 'CANCELLED', 'INTERRUPTED']

onMounted(async () => {
  await store.refresh()
})

function applyFilter(): void {
  store.filter.status = statusFilter.value || undefined
  store.refresh()
}

function onView(job: TaskJob): void {
  selected.value = job
  drawer.value = true
}

async function onStop(job: TaskJob): Promise<void> {
  await ElMessageBox.confirm(`确认停止任务 ${job.task_no}？`, '停止任务', { type: 'warning' })
  await stopTask(job.id)
  ElMessage.success('已发送停止请求')
  store.refresh()
}

async function onPause(job: TaskJob): Promise<void> {
  await pauseTask(job.id)
  ElMessage.success('已暂停')
  store.refresh()
}

async function onResume(job: TaskJob): Promise<void> {
  await resumeTask(job.id)
  ElMessage.success('已恢复')
  store.refresh()
}
</script>

<template>
  <div class="page">
    <div class="toolbar">
      <el-select v-model="statusFilter" clearable placeholder="全部状态" style="width: 160px" @change="applyFilter">
        <el-option v-for="s in statuses" :key="s" :value="s" :label="s" />
      </el-select>
      <el-button type="primary" text @click="store.refresh">刷新</el-button>
    </div>
    <div class="card">
      <TaskTable
        :jobs="store.items"
        :loading="store.loading"
        @view="onView"
        @stop="onStop"
      />
    </div>

    <div v-if="selected" class="card" style="margin-top: 16px">
      <h3>{{ selected.task_no }} 执行记录（attempt 列表）</h3>
      <el-button size="small" :disabled="selected.status !== 'PAUSED'" @click="onResume(selected)">恢复</el-button>
      <el-button size="small" :disabled="selected.status !== 'QUEUED'" @click="onPause(selected)">暂停</el-button>
    </div>

    <LogDrawer v-model:visible="drawer" :job="selected" />
  </div>
</template>

<style scoped>
.toolbar {
  display: flex;
  gap: 12px;
  margin-bottom: 12px;
}
h3 {
  margin: 0 0 12px;
  font-size: 14px;
}
</style>
