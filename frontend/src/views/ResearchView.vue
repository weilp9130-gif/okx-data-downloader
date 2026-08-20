<template>
  <div class="page research-page">
    <div class="card hero">
      <h2>研究中心</h2>
      <p class="muted">研究能力正在规划中。当前平台优先保证采集、质量、任务和延迟监控链路稳定可靠。</p>
    </div>
    <div class="roadmap">
      <div class="card"><h3>01 · 数据准备</h3><p>资产目录、质量评分和可复现的数据切片。</p><el-tag type="success">已具备基础</el-tag></div>
      <div class="card"><h3>02 · 特征与因子</h3><p>因子定义、版本管理与批量计算。</p><el-tag type="info">规划中</el-tag></div>
      <div class="card"><h3>03 · 实验与回测</h3><p>实验追踪、模型评估和策略回测。</p><el-tag type="info">规划中</el-tag></div>
    </div>
  </div>
</template>

<style scoped>
.hero { margin-bottom: 16px; }
h2 { margin: 0 0 8px; }
h3 { margin-top: 0; }
.roadmap { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
</style>
