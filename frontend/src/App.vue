<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { Coin, DataLine, Download, List, Odometer, Setting, TrendCharts, CircleCheck, Fold, Expand } from '@element-plus/icons-vue'

import { useSystemStore } from '@/stores/system'
import { useThemeStore } from '@/stores/theme'

const route = useRoute()
const store = useSystemStore()
const theme = useThemeStore()
const collapsed = ref(false)

const menus = [
  { path: '/dashboard', label: '仪表盘', icon: Odometer },
  { path: '/assets', label: '数据资产', icon: Coin },
  { path: '/ingestion', label: '数据采集', icon: Download },
  { path: '/tasks', label: '任务中心', icon: List },
  { path: '/quality', label: '数据质量', icon: CircleCheck },
  { path: '/monitoring', label: '实时监控', icon: DataLine },
  { path: '/research', label: '研究中心', icon: TrendCharts },
  { path: '/system', label: '系统管理', icon: Setting },
]

const activePath = computed(() => route.path)
const systemOk = computed(() => store.health?.worker === true)

let timer: number | null = null
let healthTimer: number | null = null
onMounted(() => {
  store.tick()
  store.refreshHealth().catch(() => undefined)
  timer = window.setInterval(() => store.tick(), 1000)
  healthTimer = window.setInterval(() => store.refreshHealth().catch(() => undefined), 20000)
})
onBeforeUnmount(() => {
  if (timer !== null) window.clearInterval(timer)
  if (healthTimer !== null) window.clearInterval(healthTimer)
})
</script>

<template>
  <el-container class="layout">
    <el-aside :width="collapsed ? '64px' : '220px'" class="aside">
      <div class="logo">
        <span class="logo-mark">Q</span>
        <span v-if="!collapsed">OKX Quant Platform</span>
      </div>
      <el-menu :default-active="activePath" :collapse="collapsed" router class="menu">
        <el-menu-item v-for="m in menus" :key="m.path" :index="m.path">
          <el-icon><component :is="m.icon" /></el-icon>
          <span>{{ m.label }}</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header class="header">
        <div class="page-title">{{ route.meta.title ?? '' }}</div>
        <div class="header-right">
          <el-button text :icon="collapsed ? Expand : Fold" @click="collapsed = !collapsed" />
          <span class="status-dot" :class="systemOk ? 'status-success' : 'status-warning'" />
          <span class="muted">{{ systemOk ? 'Worker 在线' : 'Worker 离线' }}</span>
          <el-select :model-value="theme.mode" size="small" class="theme-select" @update:model-value="theme.setMode($event)">
            <el-option label="跟随系统" value="system" />
            <el-option label="浅色" value="light" />
            <el-option label="深色" value="dark" />
          </el-select>
          <span class="clock mono">{{ store.now.toLocaleTimeString() }}</span>
        </div>
      </el-header>
      <el-main class="main">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<style scoped>
.layout {
  height: 100vh;
}
.aside {
  background: var(--bg-card);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
}
.logo {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 16px;
  font-weight: 700;
  font-size: 15px;
}
.theme-select { width: 102px; }
.logo-mark {
  background: var(--color-primary);
  color: #fff;
  width: 26px;
  height: 26px;
  border-radius: 6px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-weight: 800;
}
.menu {
  border-right: none;
  background: transparent;
}
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--border);
  background: var(--bg-card);
}
.page-title {
  font-size: 16px;
  font-weight: 600;
}
.header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}
.clock {
  font-size: 13px;
}
.main {
  padding: 0;
  overflow: auto;
  background: var(--bg-main);
}
</style>
