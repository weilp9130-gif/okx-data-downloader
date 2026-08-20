<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'

import { getTaskAttempts, getTaskLog, type TaskAttempt, type TaskJob } from '@/api/tasks'

const props = defineProps<{ visible: boolean; job: TaskJob | null }>()

const emit = defineEmits<{ (e: 'update:visible', v: boolean): void }>()

const logContent = ref('')
const offset = ref(0)
const attempts = ref<TaskAttempt[]>([])
const selectedAttempt = ref<number | undefined>()
let timer: number | null = null
let alive = true

async function loadLog(): Promise<void> {
  if (!props.job) return
  try {
    const res = await getTaskLog(props.job.id, offset.value, selectedAttempt.value)
    if (res.content) {
      logContent.value += res.content
      offset.value = res.offset
    }
    const el = document.querySelector('.log-body')
    if (el) el.scrollTop = el.scrollHeight
  } catch {
    /* ignore */
  }
}

function reset(): void {
  logContent.value = ''
  offset.value = 0
  selectedAttempt.value = undefined
  attempts.value = []
}

async function loadAttempts(): Promise<void> {
  if (!props.job) return
  try {
    attempts.value = await getTaskAttempts(props.job.id)
  } catch {
    /* ignore */
  }
}

watch(
  () => props.visible,
  (v) => {
    if (v && props.job) {
      reset()
      loadAttempts()
      loadLog()
      timer = window.setInterval(loadLog, 2000)
    } else if (!v && timer !== null) {
      window.clearInterval(timer)
      timer = null
    }
  },
)

watch(selectedAttempt, () => {
  logContent.value = ''
  offset.value = 0
  loadLog()
})

onBeforeUnmount(() => {
  alive = false
  if (timer !== null) window.clearInterval(timer)
})
</script>

<template>
  <el-drawer
    :model-value="visible"
    :title="job ? `${job.task_no} · ${job.task_type}` : '日志'"
    size="60%"
    @update:model-value="emit('update:visible', $event)"
  >
    <div class="attempt-bar">
      <span class="muted">attempt：</span>
      <el-select v-model="selectedAttempt" size="small" style="width: 120px" clearable placeholder="最近">
        <el-option v-for="a in attempts" :key="a.id" :value="a.attempt_no" :label="`attempt-${a.attempt_no}`" />
      </el-select>
    </div>
    <div class="log-body mono">
      <pre>{{ logContent || '(暂无日志)' }}</pre>
    </div>
  </el-drawer>
</template>

<style scoped>
.log-body {
  background: #0a0f1a;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 10px;
  height: calc(100vh - 180px);
  overflow: auto;
  font-size: 12px;
  line-height: 1.5;
}
.log-body pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  color: #c9d4e6;
}
.attempt-bar {
  margin-bottom: 10px;
}
</style>
