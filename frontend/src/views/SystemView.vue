<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'

import { getLogFiles, getSystemInfo, readLog, type SystemInfo } from '@/api/system'
import { useSystemStore } from '@/stores/system'
import { useLatencyStore } from '@/stores/latency'

const store = useSystemStore()
const latencyStore = useLatencyStore()
const logFiles = ref<Array<{ name: string; size: number; modified: number }>>([])
const logContent = ref('')
const logOffset = ref(0)
const currentLog = ref('')
const audit = ref<Array<Record<string, unknown>>>([])
let timer: number | null = null

function fmtSize(n: number): string {
  return n > 1024 ? `${(n / 1024).toFixed(1)} KB` : `${n} B`
}

async function loadLog(file: string): Promise<void> {
  currentLog.value = file
  logOffset.value = 0
  logContent.value = ''
  const r = await readLog(file, 0)
  logContent.value = r.content
  logOffset.value = r.offset
}

async function pollLog(): Promise<void> {
  if (!currentLog.value) return
  try {
    const r = await readLog(currentLog.value, logOffset.value)
    if (r.content) {
      logContent.value += r.content
      logOffset.value = r.offset
    }
  } catch {
    /* ignore */
  }
}

onMounted(async () => {
  store.refreshInfo()
  store.refreshDashboard()
  latencyStore.refreshWorkers()
  logFiles.value = (await getLogFiles()).files
  audit.value = (await (await fetch('/api/audit?limit=30')).json()).items
  timer = window.setInterval(pollLog, 3000)
})

onBeforeUnmount(() => {
  if (timer !== null) window.clearInterval(timer)
})
</script>

<template>
  <div class="page grid">
    <div class="card">
      <h3>只读配置（密钥打码）</h3>
      <template v-if="store.info">
        <el-descriptions :column="1" size="small" border>
          <el-descriptions-item label="版本">{{ store.info.version }}</el-descriptions-item>
          <el-descriptions-item label="Python">{{ store.info.python }}</el-descriptions-item>
          <el-descriptions-item label="平台">{{ store.info.platform }}</el-descriptions-item>
          <el-descriptions-item label="数据库">{{ store.info.db_host }}/{{ store.info.db_name }}</el-descriptions-item>
          <el-descriptions-item label="OKX 沙箱">{{ store.info.okx['sandbox'] }}</el-descriptions-item>
          <el-descriptions-item v-for="(v, k) in store.info.masked_keys" :key="k" :label="k">{{ v || '(未设置)' }}</el-descriptions-item>
        </el-descriptions>
      </template>
    </div>

    <div class="card">
      <h3>Worker</h3>
      <el-table :data="latencyStore.workers" size="small" max-height="260">
        <el-table-column prop="name" label="名称" width="90" />
        <el-table-column prop="hostname" label="主机" width="140" />
        <el-table-column prop="ip" label="IP" width="110" />
        <el-table-column prop="status" label="状态" width="80" />
        <el-table-column prop="os" label="OS" min-width="150" show-overflow-tooltip />
      </el-table>
    </div>

    <div class="card">
      <h3>审计日志</h3>
      <el-table :data="audit" size="small" max-height="260">
        <el-table-column prop="ts" label="时间" width="160">
          <template #default="{ row }">{{ new Date(row.ts).toLocaleString() }}</template>
        </el-table-column>
        <el-table-column prop="action" label="动作" width="140" />
        <el-table-column prop="target_type" label="目标" width="90" />
        <el-table-column prop="target_id" label="ID" min-width="150" show-overflow-tooltip />
      </el-table>
    </div>

    <div class="card log-card">
      <h3>日志</h3>
      <div class="log-files">
        <el-select v-model="currentLog" style="width: 100%" placeholder="选择日志文件" @change="(v: string) => loadLog(v)">
          <el-option v-for="f in logFiles" :key="f.name" :value="f.name" :label="`${f.name} (${fmtSize(f.size)})`" />
        </el-select>
      </div>
      <div class="log-body mono">
        <pre>{{ logContent || '(选择日志文件)' }}</pre>
      </div>
    </div>
  </div>
</template>

<style scoped>
.grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}
.log-card {
  grid-column: 1 / 3;
}
h3 {
  margin: 0 0 12px;
  font-size: 14px;
}
.log-body {
  background: #0a0f1a;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 10px;
  height: 320px;
  overflow: auto;
  font-size: 12px;
  line-height: 1.5;
  margin-top: 10px;
}
.log-body pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  color: #c9d4e6;
}
</style>
