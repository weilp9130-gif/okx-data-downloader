<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'

import { getBars, getInstruments } from '@/api/system'
import { submitBatch } from '@/api/tasks'
import { runInstrumentsSync } from '@/api/monitor'

const datasets = [
  { value: 'KLINE', label: 'K线' },
  { value: 'TRADES', label: '成交明细' },
  { value: 'FUNDING_RATE', label: '资金费率' },
  { value: 'MARK_PRICE', label: '标记价格' },
  { value: 'INDEX_PRICE', label: '指数价格' },
  { value: 'OPEN_INTEREST', label: '持仓量' },
]

const dataset = ref('KLINE')
const insts = ref<string[]>([])
const instOptions = ref<string[]>([])
const bars = ref<string[]>(['1D'])
const barOptions = ref<string[]>([])
const start = ref('')
const end = ref('')
const strategy = ref('patch')
const maxPages = ref(10)
const loading = ref(false)

const showBars = computed(() => dataset.value === 'KLINE')
const showTime = computed(() => !['OPEN_INTEREST', 'INSTRUMENTS'].includes(dataset.value))
const showStrategy = computed(() => dataset.value === 'KLINE')
const showMaxPages = computed(() => dataset.value === 'TRADES')

async function refreshInstruments(): Promise<void> {
  instOptions.value = (await getInstruments({ inst_type: 'SWAP' })).items
}

function defaultTimeRange(): void {
  const d = new Date()
  end.value = d.toISOString().slice(0, 10)
  d.setDate(d.getDate() - 7)
  start.value = d.toISOString().slice(0, 10)
}

function validTimeRange(): boolean {
  if (!showTime.value || !start.value || !end.value) return true
  if (start.value > end.value) {
    ElMessage.warning('开始日期不能晚于结束日期')
    return false
  }
  return true
}

watch(dataset, () => {
  if (dataset.value === 'MARK_PRICE' || dataset.value === 'INDEX_PRICE') {
    bars.value = ['1D']
  }
  if (dataset.value === 'OPEN_INTEREST') {
    start.value = ''
    end.value = ''
  }
})

async function submit(): Promise<void> {
  if (insts.value.length === 0) {
    ElMessage.warning('请选择交易对')
    return
  }
  if (!validTimeRange()) return
  const paramsList = insts.value.map((inst) => {
    const base = { inst }
    if (dataset.value === 'KLINE') {
      return { ...base, bars: bars.value, start: start.value, end: end.value, strategy: strategy.value }
    }
    if (dataset.value === 'TRADES') {
      return { ...base, start: start.value, end: end.value, max_pages: maxPages.value }
    }
    if (dataset.value === 'FUNDING_RATE') {
      return { ...base, start: start.value, end: end.value }
    }
    if (dataset.value === 'MARK_PRICE' || dataset.value === 'INDEX_PRICE') {
      return { ...base, bar: bars.value[0] ?? '1D', start: start.value, end: end.value }
    }
    if (dataset.value === 'OPEN_INTEREST') {
      return base
    }
    return base
  })

  loading.value = true
  try {
    const res = await submitBatch(dataset.value, paramsList)
    ElMessage.success(`已提交 ${res.task_ids.length} 个任务（group ${res.group_id.slice(0, 8)}…）`)
  } catch (e) {
    ElMessage.error(`提交失败: ${(e as Error).message}`)
  } finally {
    loading.value = false
  }
}

async function syncInstruments(): Promise<void> {
  await runInstrumentsSync()
  ElMessage.success('交易对同步任务已创建')
}

onMounted(async () => {
  await Promise.all([refreshInstruments(), (async () => { barOptions.value = (await getBars()).items })()])
  defaultTimeRange()
})
</script>

<template>
  <div class="page">
    <div class="card form-card">
      <h3>数据采集</h3>
      <el-form label-width="90px">
        <el-form-item label="数据集">
          <el-select v-model="dataset" style="width: 240px">
            <el-option v-for="d in datasets" :key="d.value" :value="d.value" :label="d.label" />
          </el-select>
        </el-form-item>

        <el-form-item label="交易对">
          <el-select v-model="insts" multiple filterable style="width: 100%" placeholder="选择 SWAP 交易对">
            <el-option v-for="i in instOptions" :key="i" :value="i" :label="i" />
          </el-select>
        </el-form-item>

        <el-form-item v-if="showBars" label="周期">
          <el-select v-model="bars" multiple style="width: 100%">
            <el-option v-for="b in barOptions" :key="b" :value="b" :label="b" />
          </el-select>
        </el-form-item>

        <el-form-item v-if="showTime" label="时间范围">
          <el-date-picker v-model="start" type="date" placeholder="开始" style="width: 200px" value-format="YYYY-MM-DD" />
          <span class="sep">~</span>
          <el-date-picker v-model="end" type="date" placeholder="结束" style="width: 200px" value-format="YYYY-MM-DD" />
        </el-form-item>

        <el-form-item v-if="showStrategy" label="策略">
          <el-radio-group v-model="strategy">
            <el-radio value="patch">补缺</el-radio>
            <el-radio value="full">覆盖</el-radio>
            <el-radio value="incremental">近1天增量</el-radio>
          </el-radio-group>
        </el-form-item>

        <el-form-item v-if="showMaxPages" label="最大页数">
          <el-input-number v-model="maxPages" :min="1" :max="100" />
        </el-form-item>

        <el-form-item>
          <el-button type="primary" :loading="loading" @click="submit">提交任务</el-button>
          <el-button @click="syncInstruments">同步交易对信息</el-button>
        </el-form-item>
      </el-form>
    </div>
  </div>
</template>

<style scoped>
.form-card {
  max-width: 720px;
}
h3 {
  margin: 0 0 16px;
  font-size: 14px;
}
.sep {
  margin: 0 8px;
}
</style>
