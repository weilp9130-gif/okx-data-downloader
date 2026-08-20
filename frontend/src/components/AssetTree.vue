<script setup lang="ts">
import { computed } from 'vue'

import type { AssetTreeItem } from '@/api/assets'

const props = defineProps<{ tree: AssetTreeItem[] }>()

const emit = defineEmits<{ (e: 'select', assetId: number): void }>()

const statusTone = computed(() => ({
  HEALTHY: 'success',
  WARNING: 'warning',
  STALE: 'danger',
  ERROR: 'danger',
  NO_DATA: 'muted',
}) as Record<string, string>)

const statusClass = (status: string): string => `status-${statusTone.value[status] ?? 'muted'}`
</script>

<template>
  <div class="asset-tree">
    <el-tree
      :data="props.tree"
      node-key="inst_id"
      default-expand-all
      :expand-on-click-node="false"
    >
      <template #default="{ node, data }">
        <div class="tree-node">
          <span class="inst">{{ data.inst_id }}</span>
          <template v-if="data.datasets">
            <el-tag
              v-for="(ds, key) in data.datasets"
              :key="key"
              size="small"
              class="ds-tag"
              :type="ds.status === 'HEALTHY' ? 'success' : ds.status === 'WARNING' ? 'warning' : ds.status === 'NO_DATA' ? 'info' : 'danger'"
              effect="plain"
              @click.stop="emit('select', ds.asset_id)"
            >
              {{ ds.dataset }}{{ ds.bar ? '/' + ds.bar : '' }}
            </el-tag>
          </template>
          <span v-else class="muted leaf">{{ node.label }}</span>
        </div>
      </template>
    </el-tree>
  </div>
</template>

<style scoped>
.tree-node {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  width: 100%;
}
.inst {
  font-weight: 600;
  min-width: 140px;
}
.ds-tag {
  cursor: pointer;
}
.leaf {
  margin-left: 4px;
}
</style>
