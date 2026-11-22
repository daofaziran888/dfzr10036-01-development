<template>
  <section class="page coverage-page" data-testid="coverage-page">
    <header class="page-head">
      <div>
        <h2>回填区间数据视图</h2>
        <p class="page-desc">
          按数据流归并乱序丢弃区间与在途回填批次，扣除已落库范围，按区间呈现
          已覆盖 / 占用 / 待派发，供值班员从缺口发起增量回填。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn ghost" type="button" @click="resetScenario">重置演练场景</button>
        <button class="btn" type="button" @click="reloadAll">刷新视图</button>
      </div>
    </header>

    <div class="filter-bar">
      <label class="filter-item">
        <span>数据流</span>
        <select v-model="stream" data-testid="stream-select" @change="reloadAll">
          <option v-for="item in streams" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
    </div>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">缺口位点</span>
        <strong class="stat-value">{{ view?.counts.gap ?? 0 }}</strong>
      </article>
      <article class="stat-card covered">
        <span class="stat-label">已覆盖（落库回执）</span>
        <strong class="stat-value">{{ view?.counts.covered ?? 0 }}</strong>
      </article>
      <article class="stat-card occupied">
        <span class="stat-label">占用（在途租约）</span>
        <strong class="stat-value">{{ view?.counts.occupied ?? 0 }}</strong>
      </article>
      <article class="stat-card dispatchable">
        <span class="stat-label">待派发</span>
        <strong class="stat-value">{{ view?.counts.dispatchable ?? 0 }}</strong>
      </article>
    </div>

    <h3>区间分段（已覆盖 / 占用 / 待派发）</h3>
    <table class="data-table" data-testid="segment-table">
      <thead>
        <tr><th>区间</th><th>状态</th><th>有效写入方 / 说明</th><th>位点数</th></tr>
      </thead>
      <tbody>
        <tr v-for="seg in view?.segments ?? []" :key="`${seg.lo}-${seg.state}-${seg.owner}`"
            :data-state="seg.state" :data-testid="`seg-${seg.range}`">
          <td>{{ seg.range }}</td>
          <td><span class="state-badge" :class="badgeClass(seg.state)">{{ seg.state }}</span></td>
          <td>{{ seg.owner }}</td>
          <td>{{ seg.length }}</td>
        </tr>
      </tbody>
    </table>

    <h3>占用位点与有效写入方（同一未提交位点仅一笔有效回填）</h3>
    <table class="data-table" data-testid="occupancy-table">
      <thead>
        <tr><th>位点</th><th>有效写入批次</th><th>执行端</th><th>连接状态</th><th>批次状态</th></tr>
      </thead>
      <tbody>
        <tr v-for="occ in view?.occupancy ?? []" :key="occ.pos" :data-testid="`occ-${occ.pos}`">
          <td>{{ occ.pos }}</td>
          <td>{{ occ.valid_writer }}</td>
          <td>{{ occ.executor }}</td>
          <td>{{ occ.connected ? '已连接' : '失联' }}</td>
          <td>{{ occ.batch_status }}</td>
        </tr>
        <tr v-if="!(view?.occupancy.length)">
          <td colspan="5" class="empty-state">当前没有被占用的位点</td>
        </tr>
      </tbody>
    </table>

    <h3>发起增量回填</h3>
    <form class="filter-bar dispatch-form" @submit.prevent="submitDispatch">
      <label class="filter-item"><span>起点</span>
        <input v-model.number="dispatchForm.lo" data-testid="dispatch-lo" type="number" required />
      </label>
      <label class="filter-item"><span>终点</span>
        <input v-model.number="dispatchForm.hi" data-testid="dispatch-hi" type="number" required />
      </label>
      <label class="filter-item"><span>执行端</span>
        <input v-model="dispatchForm.executor" data-testid="dispatch-executor" required />
      </label>
      <label class="filter-item"><span>批次号（可选）</span>
        <input v-model="dispatchForm.batch_no" data-testid="dispatch-batch" placeholder="留空自动生成" />
      </label>
      <button class="btn primary" data-testid="dispatch-submit" type="submit">派发回填</button>
    </form>
    <p v-if="lastDispatch" class="dispatch-result" data-testid="dispatch-result">
      批次 {{ lastDispatch.batch_no }} 当次受理：
      <strong>{{ lastDispatch.accepted_ranges.join('、') || '无' }}</strong>
      （{{ lastDispatch.accepted_count }} 个位点）；
      阻挡 {{ lastDispatch.blocked.length }} 段：
      <span v-for="(b, idx) in lastDispatch.blocked" :key="idx">
        {{ b.range }}（{{ b.reason }}{{ b.owner ? `，${b.owner}` : '' }}）；
      </span>
    </p>

    <h3>写入回执与中断（执行端演练）</h3>
    <form class="filter-bar" @submit.prevent="submitWrites">
      <label class="filter-item"><span>批次号</span>
        <input v-model="writeForm.batch_no" data-testid="write-batch" required />
      </label>
      <label class="filter-item"><span>位点（逗号分隔）</span>
        <input v-model="writeForm.positions" data-testid="write-positions" placeholder="如 121,122,123" required />
      </label>
      <label class="filter-item"><span>执行端</span>
        <input v-model="writeForm.executor" data-testid="write-executor" />
      </label>
      <button class="btn primary" data-testid="write-submit" type="submit">提交回执</button>
    </form>
    <div class="page-actions write-actions">
      <button class="btn" type="button" data-testid="reconnect-old"
              @click="reconnectOld">旧执行端重连（BF-OLD-0001）</button>
      <button class="btn" type="button" data-testid="interrupt-new"
              @click="interruptNew">中断新批次（BF-NEW-0001）</button>
    </div>
    <p v-if="lastWrite" class="dispatch-result" data-testid="write-result">
      接受 {{ lastWrite.accepted.join('、') || '无' }}；
      幂等 {{ lastWrite.idempotent.join('、') || '无' }}；
      拒绝 <span v-for="(r, idx) in lastWrite.rejected" :key="idx">{{ r.pos }}（{{ r.reason }}）；</span>
    </p>

    <h3>派发记录对照</h3>
    <table class="data-table" data-testid="dispatch-table">
      <thead>
        <tr><th>序号</th><th>批次号</th><th>执行端</th><th>请求范围</th><th>当次受理范围</th><th>阻挡</th></tr>
      </thead>
      <tbody>
        <tr v-for="d in dispatches" :key="d.dispatch_id" :data-testid="`dispatch-row-${d.dispatch_id}`">
          <td>{{ d.dispatch_id }}</td>
          <td>{{ d.batch_no }}</td>
          <td>{{ d.executor }}</td>
          <td>{{ d.request_range }}</td>
          <td>{{ d.accepted_ranges.join('、') || '无' }}</td>
          <td>
            <span v-for="(b, idx) in d.blocked" :key="idx">
              {{ b.range }} {{ b.reason }}{{ b.owner ? `（${b.owner}）` : '' }}；
            </span>
          </td>
        </tr>
        <tr v-if="!dispatches.length"><td colspan="6" class="empty-state">暂无派发记录</td></tr>
      </tbody>
    </table>

    <h3>目标表落库行（核对写入方与唯一性）</h3>
    <table class="data-table" data-testid="target-table">
      <thead>
        <tr><th>数据流</th><th>位点</th><th>写入批次</th><th>执行端</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in targetRows" :key="`${row.stream}-${row.pos}`"
            :data-testid="`target-row-${row.pos}`">
          <td>{{ row.stream }}</td>
          <td>{{ row.pos }}</td>
          <td>{{ row.writer_batch }}</td>
          <td>{{ row.executor }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  dispatchBackfill,
  fetchCoverageStreams,
  fetchCoverageView,
  fetchDispatches,
  fetchTargetRows,
  interruptBackfill,
  receiveBackfillWrites,
  resetCoverageScenario,
  updateBatchConnection,
  type CoverageView,
  type DispatchRecord,
  type TargetRow,
} from '@/api/backend-service'

const ORDER_STREAM = 'order_events_cdc'
const streams = ref<string[]>([])
const stream = ref(ORDER_STREAM)
const view = ref<CoverageView | null>(null)
const dispatches = ref<DispatchRecord[]>([])
const targetRows = ref<TargetRow[]>([])
const errorMessage = ref('')
const lastDispatch = ref<DispatchRecord | null>(null)
const lastWrite = ref<{ accepted: number[]; idempotent: number[]; rejected: { pos: number; reason: string }[] } | null>(null)

const dispatchForm = reactive({ lo: 101, hi: 130, executor: '新执行端', batch_no: '' })
const writeForm = reactive({ batch_no: 'BF-NEW-0001', positions: '121,122,123,124', executor: '新执行端' })

function badgeClass(state: string): string {
  if (state === '已覆盖') return 'badge-covered'
  if (state === '占用') return 'badge-occupied'
  return 'badge-dispatchable'
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [currentView, dispatchList, rows] = await Promise.all([
      fetchCoverageView(stream.value),
      fetchDispatches(stream.value),
      fetchTargetRows(stream.value),
    ])
    view.value = currentView
    dispatches.value = dispatchList
    targetRows.value = rows
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '视图加载失败'
  }
}

async function resetScenario() {
  errorMessage.value = ''
  try {
    await resetCoverageScenario()
    streams.value = await fetchCoverageStreams()
    stream.value = streams.value.includes(ORDER_STREAM) ? ORDER_STREAM : (streams.value[0] ?? '')
    lastDispatch.value = null
    lastWrite.value = null
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重置失败'
  }
}

async function submitDispatch() {
  errorMessage.value = ''
  try {
    const result = await dispatchBackfill({
      stream: stream.value,
      lo: Number(dispatchForm.lo),
      hi: Number(dispatchForm.hi),
      executor: dispatchForm.executor,
      batch_no: dispatchForm.batch_no || undefined,
    })
    lastDispatch.value = result.dispatch
    view.value = result.view
    await Promise.all([
      fetchDispatches(stream.value).then((items) => (dispatches.value = items)),
      fetchTargetRows(stream.value).then((items) => (targetRows.value = items)),
    ])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '派发失败'
  }
}

async function submitWrites() {
  errorMessage.value = ''
  try {
    const positions = writeForm.positions
      .split(',')
      .map((item) => Number(item.trim()))
      .filter((item) => Number.isInteger(item))
    const result = await receiveBackfillWrites({
      stream: stream.value,
      batch_no: writeForm.batch_no,
      positions,
      executor: writeForm.executor || undefined,
    })
    lastWrite.value = result
    view.value = result.view
    await fetchTargetRows(stream.value).then((items) => (targetRows.value = items))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执提交失败'
  }
}

async function reconnectOld() {
  await updateBatchConnection({ batch_no: 'BF-OLD-0001', connected: true })
  await reloadAll()
}

async function interruptNew() {
  errorMessage.value = ''
  try {
    const result = await interruptBackfill({ stream: stream.value, batch_no: 'BF-NEW-0001' })
    view.value = result.view
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '中断失败'
  }
}

onMounted(async () => {
  streams.value = await fetchCoverageStreams()
  if (!streams.value.includes(stream.value) && streams.value.length) {
    stream.value = streams.value[0]
  }
  await reloadAll()
})
</script>

<style scoped>
.coverage-page h3 {
  margin: 18px 0 8px;
  font-size: 15px;
}
.state-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
}
.badge-covered {
  background: #e6f7ed;
  color: #1a7f4b;
}
.badge-occupied {
  background: #fff2e0;
  color: #b25b00;
}
.badge-dispatchable {
  background: #e8f0fe;
  color: #1a56c4;
}
.stat-card.covered { border-left: 3px solid #1a7f4b; }
.stat-card.occupied { border-left: 3px solid #b25b00; }
.stat-card.dispatchable { border-left: 3px solid #1a56c4; }
.dispatch-form { flex-wrap: wrap; }
.dispatch-result {
  margin: 8px 0;
  padding: 8px 10px;
  background: #f7f9fc;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
}
.write-actions { margin-top: 8px; gap: 8px; }
</style>
