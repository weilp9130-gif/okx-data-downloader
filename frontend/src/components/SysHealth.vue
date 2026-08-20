<script setup lang="ts">
import type { DashboardData } from '@/stores/system'

defineProps<{ health: DashboardData['health'] }>()

const items = (h: DashboardData['health']) => [
  { key: 'db', label: '数据库', ok: h.db },
  { key: 'timescale', label: 'TimescaleDB', ok: h.timescale },
  { key: 'okx_rest', label: 'OKX REST', ok: h.okx_rest === true },
  { key: 'okx_ws', label: 'OKX WS', ok: h.okx_ws === true },
  { key: 'worker', label: 'Worker', ok: h.worker },
]
</script>

<template>
  <div class="sys-health">
    <div v-for="it in items(health)" :key="it.key" class="item">
      <span class="dot" :class="it.ok ? 'ok' : 'bad'" />
      <span>{{ it.label }}</span>
      <span class="val" :class="it.ok ? 'ok' : 'bad'">{{ it.ok ? '正常' : '异常' }}</span>
    </div>
  </div>
</template>

<style scoped>
.sys-health {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.item {
  display: flex;
  align-items: center;
  gap: 8px;
}
.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}
.ok {
  background: var(--color-success);
  color: var(--color-success);
}
.bad {
  background: var(--color-danger);
  color: var(--color-danger);
}
.val {
  margin-left: auto;
  font-size: 12px;
}
</style>
