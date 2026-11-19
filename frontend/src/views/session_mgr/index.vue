<template>
  <section class="page" data-module="session_mgr">
    <header class="page-head">
      <div>
        <h2>会话管理管理</h2>
        <p class="page-desc">维护采集会话的登录凭证、超时与状态轮转，为会话管理提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记采集会话</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出会话管理清单</button>
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
            <button class="link" type="button" :data-testid="`consume-${row.id}`" @click="openConsumePanel(row)">
              消费配额批次
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无会话管理数据，可先登记采集会话</td>
        </tr>
      </tbody>
    </table>

    <div v-if="consumeTarget" class="quota-panel" data-testid="consume-panel">
      <header class="page-head">
        <div>
          <h3>配额审计批次消费 · {{ consumeTarget['会话编号'] }}</h3>
          <p class="page-desc">
            只把已获得稳定配额序号且结果为「允许请求」的审计项写入会话日志；
            拒绝请求、幂等重放与重复消费均跳过。
          </p>
        </div>
        <div class="page-actions">
          <button class="btn ghost" type="button" @click="consumeTarget = null">关闭面板</button>
        </div>
      </header>

      <form class="analysis-form" @submit.prevent="consumeBatch">
        <label>
          限流策略ID
          <input v-model.number="rateLimitId" data-testid="consume-rate-id" type="number" min="1" required />
        </label>
        <label>
          配额审计批次ID
          <input v-model="batchId" data-testid="consume-batch-id" placeholder="例如 ui-1700000000000" required />
        </label>
        <button class="btn primary" data-testid="consume-submit" type="button" :disabled="consuming" @click="consumeBatch">
          {{ consuming ? '消费中…' : '消费审计批次' }}
        </button>
      </form>

      <div v-if="consumeResult" class="quota-result" data-testid="consume-result">
        <p class="page-desc">
          写入日志 <strong>{{ consumeResult.written }}</strong> 条 ·
          跳过拒绝 {{ consumeResult.skipped_denied }} 条 ·
          跳过重放 {{ consumeResult.skipped_replay }} 条 ·
          跳过重复 {{ consumeResult.skipped_duplicate }} 条 ·
          会话请求计数 {{ consumeResult.request_count }}
        </p>
      </div>

      <h4>会话请求日志（与列表页/详情页同源）</h4>
      <table class="data-table" data-testid="session-request-log">
        <thead>
          <tr><th>计数</th><th>URL</th><th>配额序号</th><th>审计ID</th><th>幂等键</th><th>判定</th><th>剩余配额</th></tr>
        </thead>
        <tbody>
          <tr v-for="log in requestLogs" :key="`${log.audit_id ?? 'legacy'}-${log.count}`" :data-testid="`log-row-${log.count}`">
            <td>{{ log.count }}</td>
            <td>{{ log.url }}</td>
            <td>{{ log.quota_seq ?? '—' }}</td>
            <td>{{ log.audit_id ?? '—' }}</td>
            <td>{{ log.idempotency_key ?? '—' }}</td>
            <td>{{ log.decision ?? '普通记录' }}</td>
            <td>{{ log.remaining_quota ?? '—' }}</td>
          </tr>
          <tr v-if="!requestLogs.length">
            <td colspan="7" class="empty-state">该会话暂无请求日志</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记采集会话</h3>
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
      <span>共 {{ total }} 条会话管理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type RequestLog = {
  count: number
  url: string
  latency?: number
  quota_seq?: number
  audit_id?: string
  idempotency_key?: string
  decision?: string
  remaining_quota?: number
}
type ConsumeResult = {
  written: number
  skipped_denied: number
  skipped_replay: number
  skipped_duplicate: number
  request_count: number
}

const ENDPOINT = '/api/session_mgr'
const columns = ["会话编号", "关联源", "登录状态", "最后活动", "请求计数", "Cookie摘要", "UserAgent", "会话时长"]
const actions = ["标记活跃", "标记过期", "标记失效"]
const statuses = ["草稿", "活跃", "已过期", "已失效"]
const metricNames = ["采集会话总数", "活跃会话", "已过期会话"]

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

const consumeTarget = ref<Row | null>(null)
const requestLogs = ref<RequestLog[]>([])
const rateLimitId = ref<number | null>(null)
const batchId = ref('')
const consuming = ref(false)
const consumeResult = ref<ConsumeResult | null>(null)

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
    link.download = 'session_mgr.csv'
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
      throw new Error('会话管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话管理操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('采集会话列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    if (consumeTarget.value) {
      const refreshed = rows.value.find((row) => String(row.id) === String(consumeTarget.value?.id))
      if (refreshed) consumeTarget.value = refreshed
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话管理列表读取失败'
  }
}

async function openConsumePanel(row: Row) {
  consumeTarget.value = row
  consumeResult.value = null
  batchId.value = ''
  await loadRequestLog()
}

async function loadRequestLog() {
  if (!consumeTarget.value) return
  try {
    const response = await request(`${ENDPOINT}/${consumeTarget.value.id}`)
    if (!response.ok) throw new Error('会话明细读取失败')
    const payload = await response.json()
    requestLogs.value = (payload._request_log ?? []) as RequestLog[]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话请求日志读取失败'
  }
}

async function consumeBatch() {
  if (!consumeTarget.value || !rateLimitId.value || !batchId.value) {
    errorMessage.value = '请填写限流策略ID与配额审计批次ID'
    return
  }
  consuming.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${consumeTarget.value.id}/consume-quota-batch`, {
      method: 'POST',
      body: JSON.stringify({ rate_limit_id: rateLimitId.value, batch_id: batchId.value }),
    })
    const payload = await response.json()
    if (!response.ok) throw new Error(payload.detail || '配额审计批次消费失败')
    consumeResult.value = payload as ConsumeResult
    await loadRequestLog()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '配额审计批次消费失败'
  } finally {
    consuming.value = false
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
</style>
