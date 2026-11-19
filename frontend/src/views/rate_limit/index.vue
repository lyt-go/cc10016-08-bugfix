<template>
  <section class="page" data-module="rate_limit">
    <header class="page-head">
      <div>
        <h2>频次控制管理</h2>
        <p class="page-desc">维护请求频次、并发与队列控制策略，为频次控制提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记限流策略</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出频次控制清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" :data-testid="`quota-${row.id}`" @click="openQuotaPanel(row)">
              配额审计批次
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无频次控制数据，可先登记限流策略</td>
        </tr>
      </tbody>
    </table>

    <div v-if="quotaTarget" class="quota-panel" data-testid="quota-panel">
      <header class="page-head">
        <div>
          <h3>配额审计批次 · {{ quotaTarget['策略编号'] }}</h3>
          <p class="page-desc">
            日请求总量 {{ quotaTarget._capacity }}，已用 {{ quotaTarget._daily_used }}，
            当前剩余配额 <strong data-testid="quota-remaining">{{ quotaRemaining }}</strong>
          </p>
        </div>
        <div class="page-actions">
          <button class="btn ghost" type="button" @click="quotaTarget = null">关闭面板</button>
        </div>
      </header>

      <form class="analysis-form" @submit.prevent="addBatchRequest">
        <label>
          请求URL
          <input v-model="draftUrl" data-testid="draft-url" placeholder="https://example.com/path" required />
        </label>
        <label>
          幂等键（可留空自动生成）
          <input v-model="draftIdem" data-testid="draft-idem" placeholder="idem-key" />
        </label>
        <label>
          请求版本
          <input v-model="draftVersion" data-testid="draft-version" placeholder="v1" />
        </label>
        <button class="btn" data-testid="add-request" type="submit">加入整组请求</button>
      </form>

      <table class="data-table" data-testid="batch-request-table">
        <thead>
          <tr><th>顺序</th><th>URL</th><th>幂等键</th><th>版本</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="(req, idx) in batchRequests" :key="idx" :data-testid="`draft-row-${idx + 1}`">
            <td>{{ idx + 1 }}</td>
            <td>{{ req.url }}</td>
            <td>{{ req.idempotency_key || '（自动生成）' }}</td>
            <td>{{ req.version || 'v1' }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="removeBatchRequest(idx)">移除</button>
            </td>
          </tr>
          <tr v-if="!batchRequests.length">
            <td colspan="5" class="empty-state">尚未加入请求，可多选、批量加入后一次提交整组</td>
          </tr>
        </tbody>
      </table>

      <div class="analysis-form">
        <label>
          重放策略
          <select v-model="retryPolicy" data-testid="retry-policy">
            <option value="reuse_first">沿用首次序号（幂等不二次扣减）</option>
            <option value="reassign">重新分配序号（旧序号保留失败审计项）</option>
          </select>
        </label>
        <label>
          新旧版本优先
          <select v-model="versionPreference" data-testid="version-preference">
            <option value="first">优先首次版本</option>
            <option value="latest">优先新版本</option>
          </select>
        </label>
        <button class="btn primary" data-testid="submit-batch" type="button" :disabled="submitting" @click="submitBatch">
          {{ submitting ? '提交中…' : '一次提交整组' }}
        </button>
        <button class="btn" data-testid="clear-batch" type="button" @click="batchRequests = []">清空整组</button>
      </div>

      <div v-if="latestBatch" class="quota-result" data-testid="quota-result">
        <h4>最近审计批次：{{ latestBatch.batch_id }}</h4>
        <p class="page-desc">
          允许 {{ latestBatch.allowed_count }} 条 · 拒绝 {{ latestBatch.denied_count }} 条 ·
          重放 {{ latestBatch.replay_count }} 条 · 实际扣减 {{ latestBatch.deducted_count }} 次 ·
          最终剩余配额 <strong data-testid="result-remaining">{{ latestBatch.remaining_quota }}</strong>
          <span v-if="latestBatch.order_violation" class="error-text"> · 检测到请求顺序错位</span>
        </p>
        <table class="data-table">
          <thead>
            <tr><th>请求顺序</th><th>稳定配额序号</th><th>判定</th><th>剩余配额</th><th>幂等键</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr
              v-for="item in latestBatch.audit_items"
              :key="`${item.audit_id}-${item.request_index}-${item.replay_count}`"
              :data-testid="`audit-row-${item.request_index}`"
              :data-allowed="String(item.allowed)"
            >
              <td>{{ item.request_index }}</td>
              <td>{{ item.quota_seq }}</td>
              <td :class="item.allowed ? 'trend-up' : 'error-text'">
                {{ item.decision }}<span v-if="item.replay">（幂等重放）</span>
              </td>
              <td>{{ item.remaining_quota }}</td>
              <td>{{ item.idempotency_key }}</td>
              <td>{{ item.reason }}</td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions">
          <button
            class="btn"
            data-testid="replay-last"
            type="button"
            :disabled="!batchRequests.length"
            @click="replayLastRequest"
          >
            重放最后一条
          </button>
        </div>
      </div>

      <h4>历史配额审计批次</h4>
      <table class="data-table" data-testid="audit-batch-list">
        <thead>
          <tr><th>批次ID</th><th>条数</th><th>允许</th><th>拒绝</th><th>重放</th><th>剩余配额</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="batch in auditBatches" :key="batch.batch_id" :data-testid="`audit-batch-${batch.batch_id}`">
            <td>{{ batch.batch_id }}</td>
            <td>{{ batch.total }}</td>
            <td>{{ batch.allowed_count }}</td>
            <td>{{ batch.denied_count }}</td>
            <td>{{ batch.replay_count }}</td>
            <td>{{ batch.remaining_quota }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="latestBatch = batch">查看逐条审计</button>
            </td>
          </tr>
          <tr v-if="!auditBatches.length">
            <td colspan="7" class="empty-state">暂无配额审计批次</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记限流策略</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条频次控制记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type AuditItem = {
  audit_id: string
  request_index: number
  quota_seq: number
  allowed: boolean
  decision: string
  replay: boolean
  replay_count: number
  remaining_quota: number
  idempotency_key: string
  reason: string
}
type AuditBatch = {
  batch_id: string
  total: number
  allowed_count: number
  denied_count: number
  replay_count: number
  deducted_count: number
  remaining_quota: number
  order_violation: boolean
  audit_items: AuditItem[]
}
type BatchRequest = { url: string; idempotency_key: string; version: string; seq: number }

const ENDPOINT = '/api/rate_limit'
const columns = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
const actions = ["提交校验", "标记生效", "停用策略"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["限流策略总数", "待校验策略", "已生效策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

const quotaTarget = ref<Row | null>(null)
const batchRequests = ref<BatchRequest[]>([])
const auditBatches = ref<AuditBatch[]>([])
const latestBatch = ref<AuditBatch | null>(null)
const submitting = ref(false)
const draftUrl = ref('')
const draftIdem = ref('')
const draftVersion = ref('v1')
const retryPolicy = ref('reuse_first')
const versionPreference = ref('first')

const quotaRemaining = computed(() => {
  const target = quotaTarget.value
  if (!target) return 0
  const capacity = Number(target._capacity ?? 0)
  const used = Number(target._daily_used ?? 0)
  return Math.max(0, capacity - used)
})

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'rate_limit.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('频次控制动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '频次控制操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('限流策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '频次控制列表读取失败'
  }
}

function addBatchRequest() {
  batchRequests.value.push({
    url: draftUrl.value,
    idempotency_key: draftIdem.value,
    version: draftVersion.value || 'v1',
    seq: batchRequests.value.length + 1,
  })
  draftUrl.value = ''
  draftIdem.value = ''
}

function removeBatchRequest(index: number) {
  batchRequests.value.splice(index, 1)
  batchRequests.value.forEach((req, idx) => { req.seq = idx + 1 })
}

async function openQuotaPanel(row: Row) {
  quotaTarget.value = row
  batchRequests.value = []
  latestBatch.value = null
  await loadAuditBatches()
}

async function loadAuditBatches() {
  if (!quotaTarget.value) return
  try {
    const response = await request(`${ENDPOINT}/${quotaTarget.value.id}/quota-batches`)
    const payload = await response.json()
    auditBatches.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '配额审计批次读取失败'
  }
}

async function submitBatch() {
  if (!quotaTarget.value || !batchRequests.value.length) {
    errorMessage.value = '请先加入至少一条批量请求'
    return
  }
  submitting.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${quotaTarget.value.id}/quota-batches`, {
      method: 'POST',
      body: JSON.stringify({
        batch_id: `ui-${Date.now()}`,
        requests: batchRequests.value,
        retry_policy: retryPolicy.value,
        version_preference: versionPreference.value,
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.detail || payload.message || '批次提交失败')
    latestBatch.value = payload as AuditBatch
    await loadAuditBatches()
    await reload()
    quotaTarget.value = rows.value.find((row) => String(row.id) === String(quotaTarget.value?.id)) ?? quotaTarget.value
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '配额审计批次提交失败'
  } finally {
    submitting.value = false
  }
}

async function replayLastRequest() {
  if (!quotaTarget.value || !batchRequests.value.length) return
  const last = batchRequests.value[batchRequests.value.length - 1]
  submitting.value = true
  try {
    const response = await request(`${ENDPOINT}/${quotaTarget.value.id}/quota-batches`, {
      method: 'POST',
      body: JSON.stringify({
        batch_id: `ui-replay-${Date.now()}`,
        requests: [{ ...last, seq: 1 }],
        retry_policy: retryPolicy.value,
        version_preference: versionPreference.value,
      }),
    })
    const payload = await response.json()
    if (!response.ok) throw new Error(payload.detail || '重放失败')
    latestBatch.value = payload as AuditBatch
    await loadAuditBatches()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '幂等重放失败'
  } finally {
    submitting.value = false
  }
}

onMounted(reload)
</script>

<style scoped>
.quota-panel {
  margin: 16px 0;
  padding: 14px;
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
}
.quota-result {
  margin: 14px 0;
  padding: 12px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.trend-up { color: #15803d; }
</style>
