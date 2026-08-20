import { createRouter, createWebHashHistory } from 'vue-router'

export const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/dashboard' },
    { path: '/dashboard', name: 'dashboard', component: () => import('@/views/DashboardView.vue'), meta: { title: '仪表盘' } },
    { path: '/assets', name: 'assets', component: () => import('@/views/DataAssetsView.vue'), meta: { title: '数据资产' } },
    { path: '/ingestion', name: 'ingestion', component: () => import('@/views/IngestionView.vue'), meta: { title: '数据采集' } },
    { path: '/tasks', name: 'tasks', component: () => import('@/views/TasksView.vue'), meta: { title: '任务中心' } },
    { path: '/quality', name: 'quality', component: () => import('@/views/QualityView.vue'), meta: { title: '数据质量' } },
    { path: '/monitoring', name: 'monitoring', component: () => import('@/views/MonitoringView.vue'), meta: { title: '实时监控' } },
    { path: '/research', name: 'research', component: () => import('@/views/ResearchView.vue'), meta: { title: '研究中心' } },
    { path: '/system', name: 'system', component: () => import('@/views/SystemView.vue'), meta: { title: '系统管理' } },
  ],
})
