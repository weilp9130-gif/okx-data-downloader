<script setup lang="ts">
defineProps<{
  title: string
  value: string | number
  unit?: string
  tone?: 'normal' | 'success' | 'warning' | 'danger'
}>()
</script>

<template>
  <div class="stat-card card">
    <div class="stat-title muted">{{ title }}</div>
    <div class="stat-value" :class="tone">
      {{ value }}<span v-if="unit" class="stat-unit">{{ unit }}</span>
    </div>
    <slot />
  </div>
</template>

<style scoped>
.stat-card {
  min-width: 180px;
}
.stat-title {
  font-size: 13px;
}
.stat-value {
  font-size: 28px;
  font-weight: 600;
  margin-top: 6px;
  color: var(--text-main);
  font-variant-numeric: tabular-nums;
}
.stat-unit {
  font-size: 14px;
  color: var(--text-muted);
  margin-left: 4px;
  font-weight: 400;
}
.success {
  color: var(--color-success);
}
.warning {
  color: var(--color-warning);
}
.danger {
  color: var(--color-danger);
}
</style>
