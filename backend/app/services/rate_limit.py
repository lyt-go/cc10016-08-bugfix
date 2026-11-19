"""频次控制业务规则：令牌桶限流、并发计数、日配额追踪、配额审计批次。

配额审计批次（quota_audit_batch）是频次控制作为主处理模块对外产出的凭证：
- 每条批量请求按提交顺序消费一个稳定配额序号（quota_seq），序号只在真正接受
  处理时分配，拒绝请求同样占用序号以保证审计行与真实请求不错位；
- 同一幂等键（idempotency_key）全局只产生一份有效扣减，重复重放复用首次序号
  与首次审计结果，绝不二次扣减；
- 批次逐条标出 allowed（允许请求）、denied（拒绝请求）与 remaining_quota
  （剩余配额），供会话管理等下游模块按审计结果消费。
"""
from __future__ import annotations

import hashlib
import threading
from typing import Any

from app.store import store

MODULE = "rate_limit"
LIST_FIELDS = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
REQUIRED_FIELDS = ["策略编号", "目标源"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]

# 配额审计批次当前结构版本，历史批次按该版本补齐缺失字段（迁移兼容）。
AUDIT_SCHEMA_VERSION = 2
# 重放策略：reuse_first=沿用首次序号（默认，幂等不二次扣减）；
# reassign=重试重新分配序号（旧序号保留为失败审计项）。
RETRY_POLICIES = ("reuse_first", "reassign")
# 新旧请求版本优先规则：first=保留首次版本（默认）；latest=优先新版本。
VERSION_PREFERENCES = ("first", "latest")

# 同一批次并发写入时的处理锁，保证只保留一份处理结果。
_batch_lock = threading.RLock()


def _parse_number(value: Any, default: int) -> int:
    try:
        return int(str(value).replace("ms", "").replace("s", "").replace("次", ""))
    except (ValueError, TypeError):
        return default


def _derive_idempotency_key(entry_id: int, index: int, request: dict[str, Any]) -> str:
    """未显式提供幂等键时，用请求内容生成稳定幂等键（同一请求重放结果一致）。"""
    explicit = str(request.get("idempotency_key") or "").strip()
    if explicit:
        return explicit
    raw = "|".join(str(request.get(field, "")) for field in ("url", "method", "session_id", "payload"))
    digest = hashlib.sha1(f"{entry_id}:{index}:{raw}".encode("utf-8")).hexdigest()[:16]
    return f"idem-{entry_id}-{digest}"


def _normalise_legacy_batches(raw: Any) -> dict[str, list[dict[str, Any]]]:
    """迁移兼容：把存量历史版本的审计批次补齐到当前结构。"""
    if not isinstance(raw, dict):
        return {}
    migrated: dict[str, list[dict[str, Any]]] = {}
    for sid, batches in raw.items():
        fixed_batches: list[dict[str, Any]] = []
        if isinstance(batches, list):
            for batch in batches:
                if not isinstance(batch, dict):
                    continue
                fixed = dict(batch)
                fixed.setdefault("schema_version", 1)
                fixed.setdefault("retry_policy", "reuse_first")
                fixed.setdefault("version_preference", "first")
                fixed.setdefault("status", "completed")
                for item in fixed.get("audit_items", []):
                    if isinstance(item, dict):
                        item.setdefault("replay_count", 0)
                        item.setdefault("replay", False)
                        item.setdefault("version", item.get("version", "v1"))
                        item.setdefault("remaining_quota", item.get("remaining_quota", 0))
                fixed_batches.append(fixed)
        migrated[str(sid)] = fixed_batches
    return migrated


class RateLimitService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("策略编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        states = store.pipeline_state("rate_limit_states") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            sid = str(d["id"])
            st = states.get(sid, {"tokens": _parse_number(d.get("并发上限"), 10), "daily_used": 0})
            d["_tokens"] = st.get("tokens")
            d["_daily_used"] = st.get("daily_used", 0)
            d["_capacity"] = _parse_number(d.get("日请求总量"), 1000)
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        states = store.pipeline_state("rate_limit_states") or {}
        st = states.get(str(entry_id), {"tokens": _parse_number(d.get("并发上限"), 10), "daily_used": 0})
        d["_tokens"] = st.get("tokens")
        d["_daily_used"] = st.get("daily_used", 0)
        return d

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        # 绑定采集源
        source_id = values.get("关联采集源ID")
        if source_id:
            try:
                sid = int(source_id)
                store.add_relation("source_to_rate_limit", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        # 初始化令牌桶
        states = store.pipeline_state("rate_limit_states") or {}
        states[str(entry["id"])] = {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0, "last_refill": "2026-11-16T00:00:00Z"}
        store.set_pipeline_state("rate_limit_states", states)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"限流策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于频次控制可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"限流策略已{action}"

    def acquire_token(self, entry_id: int) -> dict[str, Any]:
        """尝试获取一个请求令牌，返回是否允许及剩余配额。"""
        entry = store.find(MODULE, entry_id)
        if entry is None or entry.get("status") != "已生效":
            return {"allowed": False, "reason": "策略不存在或未生效"}
        states = store.pipeline_state("rate_limit_states") or {}
        sid = str(entry_id)
        st = states.get(sid, {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0})
        cap = _parse_number(entry.get("日请求总量"), 1000)
        if st["daily_used"] >= cap:
            return {"allowed": False, "reason": "日配额已耗尽", "daily_used": st["daily_used"], "capacity": cap}
        if st["tokens"] <= 0:
            return {"allowed": False, "reason": "并发令牌耗尽", "tokens": 0}
        st["tokens"] -= 1
        st["daily_used"] += 1
        states[sid] = st
        store.set_pipeline_state("rate_limit_states", states)
        return {"allowed": True, "tokens": st["tokens"], "daily_used": st["daily_used"], "capacity": cap}

    def release_token(self, entry_id: int) -> dict[str, Any]:
        """释放令牌回桶。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False}
        states = store.pipeline_state("rate_limit_states") or {}
        sid = str(entry_id)
        st = states.get(sid, {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0})
        max_tokens = _parse_number(entry.get("并发上限"), 10)
        st["tokens"] = min(max_tokens, st["tokens"] + 1)
        states[sid] = st
        store.set_pipeline_state("rate_limit_states", states)
        return {"ok": True, "tokens": st["tokens"]}

    # ------------------------------------------------------------------
    # 配额审计批次：批量、多选、一次提交整组，逐条返回处理结果
    # ------------------------------------------------------------------

    def submit_quota_batch(
        self,
        entry_id: int,
        requests: list[dict[str, Any]],
        *,
        batch_id: str | None = None,
        retry_policy: str = "reuse_first",
        version_preference: str = "first",
        idempotency_prefix: str = "",
    ) -> dict[str, Any]:
        """读取采集请求配额并校验批量请求顺序，生成配额审计批次。

        每个批量请求只能消耗一个稳定配额序号；同一幂等键的重复重放不会再次
        扣减。并发提交同一批次时整批加锁，只保留一份处理结果。
        """
        if retry_policy not in RETRY_POLICIES:
            retry_policy = "reuse_first"
        if version_preference not in VERSION_PREFERENCES:
            version_preference = "first"

        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"限流策略 {entry_id} 不存在或已归档", "audit_items": []}
        if not isinstance(requests, list) or not requests:
            return {"ok": False, "error": "批量请求不能为空", "audit_items": []}

        batch_key = str(batch_id or f"batch-{entry_id}-{len(requests)}")
        locks: set[str] = store.pipeline_state("quota_batch_locks") or set()

        with _batch_lock:
            # 并发写入同一批次：只保留一份结果，后来者直接回放已有审计批次。
            existing = self._find_batch(entry_id, batch_key)
            if existing is not None:
                return self._batch_response(entry_id, existing, concurrent_hit=True)
            if batch_key in locks:
                existing = self._find_batch(entry_id, batch_key)
                if existing is not None:
                    return self._batch_response(entry_id, existing, concurrent_hit=True)
            locks.add(batch_key)
            store.set_pipeline_state("quota_batch_locks", locks)
            try:
                return self._process_quota_batch(
                    entry_id, entry, requests, batch_key, retry_policy, version_preference,
                    idempotency_prefix)
            finally:
                locks.discard(batch_key)
                store.set_pipeline_state("quota_batch_locks", locks)

    def _process_quota_batch(
        self,
        entry_id: int,
        entry: dict[str, Any],
        requests: list[dict[str, Any]],
        batch_key: str,
        retry_policy: str,
        version_preference: str,
        idempotency_prefix: str,
    ) -> dict[str, Any]:
        states = store.pipeline_state("rate_limit_states") or {}
        sid = str(entry_id)
        st = states.get(sid, {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0})
        capacity = _parse_number(entry.get("日请求总量"), 1000)
        max_tokens = _parse_number(entry.get("并发上限"), 10)

        idempotency_index: dict[str, dict[str, Any]] = store.pipeline_state("quota_audit_idempotency") or {}
        policy_index = idempotency_index.setdefault(sid, {})
        seq_watermark: dict[str, int] = store.pipeline_state("quota_audit_seq") or {}
        next_seq = int(seq_watermark.get(sid, 0))

        audit_items: list[dict[str, Any]] = []
        order_violation = False
        allowed_count = 0
        denied_count = 0
        replay_count = 0

        for index, raw_request in enumerate(requests):
            request = raw_request if isinstance(raw_request, dict) else {"url": str(raw_request)}
            idem_key = _derive_idempotency_key(entry_id, index, request)
            if idempotency_prefix:
                idem_key = f"{idempotency_prefix}:{idem_key}"
            request_version = str(request.get("version") or "v1")
            expected_order = index + 1
            try:
                submitted_order = int(request.get("seq", expected_order))
            except (ValueError, TypeError):
                submitted_order = expected_order
            # 校验批量请求顺序：漏记/乱序序号直接判错，避免审计批次与真实请求错位。
            if submitted_order != expected_order:
                order_violation = True

            prior = policy_index.get(idem_key)
            if prior is not None:
                # 同一幂等键只能产生一份有效扣减：重放沿用首次审计结果。
                if retry_policy == "reassign" and version_preference == "latest" \
                        and request_version != str(prior.get("version", "v1")):
                    # 重新分配：重试成为新的候选请求，旧序号保留为失败审计项。
                    # 同一幂等键只保留一份有效扣减：先回滚旧序号的扣减再重新判定。
                    if prior.get("allowed"):
                        st["daily_used"] = max(0, int(st.get("daily_used", 0)) - 1)
                        st["tokens"] = min(max_tokens, int(st.get("tokens", max_tokens)) + 1)
                    prior_item = self._build_reassigned_old_item(prior, index, capacity, st)
                    next_seq += 1
                    item = self._evaluate_new_request(
                        entry_id, entry, st, capacity, max_tokens, idem_key, request_version,
                        expected_order, submitted_order, next_seq, index, request)
                    audit_items.append(prior_item)
                    audit_items.append(item)
                    # 旧序号保留为失败审计项，计入拒绝。
                    denied_count += 1
                    if item["allowed"]:
                        allowed_count += 1
                        st["daily_used"] = int(st.get("daily_used", 0)) + 1
                        st["tokens"] = max(0, int(st.get("tokens", max_tokens)) - 1)
                    else:
                        denied_count += 1
                    # 无论新候选允许还是拒绝，幂等索引都指向最新一次判定，
                    # 保证后续重放不会回退到已作废的旧序号结论。
                    policy_index[idem_key] = self._index_from_item(item)
                    seq_watermark[sid] = next_seq
                    continue

                # 默认：沿用首次序号，重试复用原配额结果，不再次扣减。
                replay_item = {
                    "audit_id": f"{batch_key}-{index + 1}",
                    "request_index": expected_order,
                    "submitted_order": submitted_order,
                    "idempotency_key": idem_key,
                    "quota_seq": prior["quota_seq"],
                    "allowed": bool(prior["allowed"]),
                    "decision": "允许请求" if prior["allowed"] else "拒绝请求",
                    "replay": True,
                    "replay_count": int(prior.get("replay_count", 0)) + 1,
                    "reason": "幂等重放：沿用首次序号与首次审计结果，不重复扣减",
                    "version": request_version,
                    "kept_version": prior.get("version", "v1"),
                    "remaining_quota": max(0, capacity - int(st.get("daily_used", 0))),
                    "daily_used": int(st.get("daily_used", 0)),
                    "capacity": capacity,
                    "url": request.get("url", ""),
                }
                prior["replay_count"] = replay_item["replay_count"]
                audit_items.append(replay_item)
                # 重放行单独计入 replay_count，不再重复计入允许/拒绝，
                # 保证 allowed_count + denied_count + replay_count == total。
                replay_count += 1
                continue

            # 新候选请求：先分配稳定配额序号，再判定配额，序号一旦分配不漏记。
            next_seq += 1
            item = self._evaluate_new_request(
                entry_id, entry, st, capacity, max_tokens, idem_key, request_version,
                expected_order, submitted_order, next_seq, index, request)
            audit_items.append(item)
            if item["allowed"]:
                allowed_count += 1
                st["daily_used"] = int(st.get("daily_used", 0)) + 1
                st["tokens"] = max(0, int(st.get("tokens", max_tokens)) - 1)
                policy_index[idem_key] = self._index_from_item(item)
            else:
                denied_count += 1
                # 拒绝的序号同样登记到幂等索引，保证重放返回同一拒绝结论。
                policy_index[idem_key] = self._index_from_item(item)
            seq_watermark[sid] = next_seq

        states[sid] = st
        store.set_pipeline_state("rate_limit_states", states)
        store.set_pipeline_state("quota_audit_idempotency", idempotency_index)
        store.set_pipeline_state("quota_audit_seq", seq_watermark)

        batch = {
            "batch_id": batch_key,
            "schema_version": AUDIT_SCHEMA_VERSION,
            "rate_limit_id": entry_id,
            "策略编号": entry.get("策略编号"),
            "retry_policy": retry_policy,
            "version_preference": version_preference,
            "status": "completed",
            "concurrent_writes": 1,
            "order_violation": order_violation,
            "total": len(audit_items),
            "allowed_count": allowed_count,
            "denied_count": denied_count,
            "replay_count": replay_count,
            "deducted_count": sum(1 for item in audit_items if item["allowed"] and not item["replay"]),
            "remaining_quota": max(0, capacity - int(st.get("daily_used", 0))),
            "daily_used": int(st.get("daily_used", 0)),
            "capacity": capacity,
            "audit_items": audit_items,
        }
        all_batches = _normalise_legacy_batches(store.pipeline_state("quota_audit_batches") or {})
        all_batches.setdefault(sid, []).append(batch)
        store.set_pipeline_state("quota_audit_batches", all_batches)
        return self._batch_response(entry_id, batch)

    def _evaluate_new_request(
        self,
        entry_id: int,
        entry: dict[str, Any],
        st: dict[str, Any],
        capacity: int,
        max_tokens: int,
        idem_key: str,
        version: str,
        expected_order: int,
        submitted_order: int,
        quota_seq: int,
        index: int,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        """对新候选请求执行配额校验，生成一条允许/拒绝审计行。"""
        daily_used = int(st.get("daily_used", 0))
        tokens = int(st.get("tokens", max_tokens))
        allowed = True
        reason = "配额充足，允许请求"
        if entry.get("status") != "已生效":
            allowed = False
            reason = "限流策略未生效，拒绝请求"
        elif daily_used >= capacity:
            allowed = False
            reason = "日配额已耗尽，拒绝请求"
        elif tokens <= 0:
            allowed = False
            reason = "并发令牌耗尽，拒绝请求"
        if submitted_order != expected_order:
            reason = f"请求顺序错位（期望第{expected_order}条，实际第{submitted_order}条）：{reason}"
        return {
            "audit_id": f"seq-{entry_id}-{quota_seq}",
            "request_index": expected_order,
            "submitted_order": submitted_order,
            "idempotency_key": idem_key,
            "quota_seq": quota_seq,
            "allowed": allowed,
            "decision": "允许请求" if allowed else "拒绝请求",
            "replay": False,
            "replay_count": 0,
            "reason": reason,
            "version": version,
            "remaining_quota": max(0, capacity - (daily_used + 1 if allowed else daily_used)),
            "daily_used_before": daily_used,
            "daily_used_after": daily_used + 1 if allowed else daily_used,
            "capacity": capacity,
            "url": request.get("url", ""),
        }

    def _build_reassigned_old_item(
        self, prior: dict[str, Any], index: int, capacity: int, st: dict[str, Any]) -> dict[str, Any]:
        """重新分配策略下，旧序号保留为失败审计项（两次独立审计行之一）。"""
        return {
            "audit_id": f"{prior['audit_id']}-revoked",
            "request_index": index + 1,
            "submitted_order": index + 1,
            "idempotency_key": prior["idempotency_key"],
            "quota_seq": prior["quota_seq"],
            "allowed": False,
            "decision": "拒绝请求",
            "replay": False,
            "replay_count": 0,
            "reason": "重试重新分配序号：旧序号保留为失败审计项，原有效扣减已作废",
            "version": prior.get("version", "v1"),
            "remaining_quota": max(0, capacity - int(st.get("daily_used", 0))),
            "daily_used": int(st.get("daily_used", 0)),
            "capacity": capacity,
            "url": "",
        }

    @staticmethod
    def _index_from_item(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "audit_id": item["audit_id"],
            "quota_seq": item["quota_seq"],
            "allowed": item["allowed"],
            "idempotency_key": item["idempotency_key"],
            "version": item.get("version", "v1"),
            "replay_count": item.get("replay_count", 0),
            "remaining_quota": item.get("remaining_quota", 0),
            "capacity": item.get("capacity", 0),
        }

    def _find_batch(self, entry_id: int, batch_key: str) -> dict[str, Any] | None:
        all_batches = _normalise_legacy_batches(store.pipeline_state("quota_audit_batches") or {})
        for batch in all_batches.get(str(entry_id), []):
            if batch.get("batch_id") == batch_key:
                return batch
        return None

    def _batch_response(self, entry_id: int, batch: dict[str, Any], *, concurrent_hit: bool = False) -> dict[str, Any]:
        if concurrent_hit:
            batch = dict(batch)
            batch["concurrent_writes"] = int(batch.get("concurrent_writes", 1)) + 1
            batch["note"] = "并发写入命中同批次，复用唯一一份审计结果"
            all_batches = _normalise_legacy_batches(store.pipeline_state("quota_audit_batches") or {})
            for stored in all_batches.get(str(entry_id), []):
                if stored.get("batch_id") == batch["batch_id"]:
                    stored["concurrent_writes"] = batch["concurrent_writes"]
            store.set_pipeline_state("quota_audit_batches", all_batches)
        return {"ok": True, **batch}

    def list_audit_batches(self, entry_id: int | None = None) -> list[dict[str, Any]]:
        """读取配额审计批次（跨模块取数入口，兼容历史版本结构）。"""
        all_batches = _normalise_legacy_batches(store.pipeline_state("quota_audit_batches") or {})
        if entry_id is not None:
            return all_batches.get(str(entry_id), [])
        return [batch for batches in all_batches.values() for batch in batches]

    def get_audit_batch(self, entry_id: int, batch_key: str) -> dict[str, Any] | None:
        return self._find_batch(entry_id, batch_key)
