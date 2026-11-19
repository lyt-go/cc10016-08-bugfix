"""会话管理业务规则：会话超时轮转、Cookie摘要刷新、请求计数追踪。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "session_mgr"
LIST_FIELDS = ["会话编号", "关联源", "登录状态", "最后活动", "请求计数", "Cookie摘要", "UserAgent", "会话时长"]
REQUIRED_FIELDS = ["会话编号", "关联源"]
STATUS_ORDER = ["草稿", "活跃", "已过期", "已失效"]
ACTION_RULES = {"标记活跃": "活跃", "标记过期": "已过期", "标记失效": "已失效"}
NEGATIVE_ACTIONS = ["标记失效"]


def _check_session_expiry(entry: dict[str, Any]) -> bool:
    """检查会话是否超过时长限制（模拟）。"""
    try:
        duration = int(str(entry.get("会话时长") or "3600").replace("s", "").replace("秒", ""))
    except (ValueError, TypeError):
        duration = 3600
    req_count = int(str(entry.get("请求计数") or "0"))
    # 模拟：请求数超过阈值或固定超时即过期
    return req_count > duration / 10


class SessionMgrService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("会话编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        logs = store.pipeline_state("session_request_logs") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            expired = _check_session_expiry(d)
            d["_expired"] = expired
            d["_request_log"] = logs.get(str(d["id"]), [])
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d["_expired"] = _check_session_expiry(d)
        logs = store.pipeline_state("session_request_logs") or {}
        d["_request_log"] = logs.get(str(entry_id), [])
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
        entry["请求计数"] = "0"
        rows.append(entry)
        # 绑定代理
        proxy_id = values.get("绑定代理ID")
        if proxy_id:
            try:
                pid = int(proxy_id)
                store.add_relation("session_to_proxy", MODULE, entry["id"], "proxy_pool", pid)
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集会话 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于会话管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采集会话已{action}"

    def _append_request_log(self, entry_id: int, log_entry: dict[str, Any]) -> list[dict[str, Any]]:
        """统一的会话请求日志写入入口，保证列表页与详情页取数同源、不错位。"""
        logs = store.pipeline_state("session_request_logs") or {}
        log = logs.setdefault(str(entry_id), [])
        log.append(log_entry)
        if len(log) > 100:
            logs[str(entry_id)] = log[-100:]
        store.set_pipeline_state("session_request_logs", logs)
        return logs[str(entry_id)]

    def record_request(self, entry_id: int, url: str, latency_ms: float) -> dict[str, Any]:
        """记录会话请求并更新计数。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "会话不存在"}
        try:
            count = int(str(entry.get("请求计数") or "0")) + 1
        except (ValueError, TypeError):
            count = 1
        entry["请求计数"] = str(count)
        entry["最后活动"] = "2026-11-16T00:00:00Z"
        self._append_request_log(entry_id, {"url": url, "latency": latency_ms, "count": count})
        return {"ok": True, "request_count": count}

    def consume_quota_batch(
        self,
        session_id: int,
        rate_limit_id: int,
        batch_id: str | None = None,
        audit_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """消费频次控制产出的配额审计批次。

        只把「已获得稳定序号且结果为允许」的请求写入会话日志；拒绝请求与
        幂等重放行不重复落日志。同一审计项只会被消费一次，跨模块衔接幂等。
        """
        from app.services.rate_limit import RateLimitService

        entry = store.find(MODULE, session_id)
        if entry is None:
            return {"ok": False, "error": f"采集会话 {session_id} 不存在或已归档"}

        # 沿会话管理取数链路查找配额审计批次依赖：优先按批次ID读取频次控制产物。
        if audit_items is None:
            rate_svc = RateLimitService()
            if batch_id is None:
                return {"ok": False, "error": "缺少配额审计批次ID或审计明细"}
            batch = rate_svc.get_audit_batch(rate_limit_id, batch_id)
            if batch is None:
                return {"ok": False, "error": f"配额审计批次 {batch_id} 不存在"}
            audit_items = batch.get("audit_items", [])
        if not isinstance(audit_items, list):
            return {"ok": False, "error": "配额审计批次明细格式不正确"}

        consumed_index = store.pipeline_state("session_quota_consumed") or {}
        session_key = str(session_id)
        consumed_audit_ids = set(consumed_index.get(session_key, []))

        written = 0
        skipped_denied = 0
        skipped_replay = 0
        skipped_duplicate = 0
        written_logs: list[dict[str, Any]] = []
        for item in audit_items:
            if not isinstance(item, dict):
                continue
            audit_id = str(item.get("audit_id") or "")
            quota_seq = item.get("quota_seq")
            # 漏记序号的审计项不能写入，避免与真实请求错位。
            if quota_seq is None:
                skipped_denied += 1
                continue
            # 幂等重放行优先归类：无论携带的首次结论是允许还是拒绝，都不重复落日志。
            if item.get("replay"):
                skipped_replay += 1
                continue
            if not item.get("allowed"):
                skipped_denied += 1
                continue
            if audit_id and audit_id in consumed_audit_ids:
                skipped_duplicate += 1
                continue

            try:
                count = int(str(entry.get("请求计数") or "0")) + 1
            except (ValueError, TypeError):
                count = 1
            entry["请求计数"] = str(count)
            entry["最后活动"] = "2026-11-16T00:00:00Z"
            log_entry = {
                "url": item.get("url", ""),
                "latency": item.get("latency", 0),
                "count": count,
                "quota_seq": quota_seq,
                "audit_id": audit_id,
                "idempotency_key": item.get("idempotency_key", ""),
                "rate_limit_id": rate_limit_id,
                "batch_id": batch_id or item.get("batch_id", ""),
                "decision": item.get("decision", "允许请求"),
                "remaining_quota": item.get("remaining_quota"),
            }
            self._append_request_log(session_id, log_entry)
            written_logs.append(log_entry)
            written += 1
            if audit_id:
                consumed_audit_ids.add(audit_id)

        consumed_index[session_key] = sorted(consumed_audit_ids)
        store.set_pipeline_state("session_quota_consumed", consumed_index)
        return {
            "ok": True,
            "session_id": session_id,
            "rate_limit_id": rate_limit_id,
            "batch_id": batch_id,
            "written": written,
            "skipped_denied": skipped_denied,
            "skipped_replay": skipped_replay,
            "skipped_duplicate": skipped_duplicate,
            "request_count": int(str(entry.get("请求计数") or "0")),
            "written_logs": written_logs,
        }

    def rotate_session(self, source_id: int) -> dict[str, Any] | None:
        """为指定采集源轮替到一个活跃会话。"""
        rels = store.get_relations("schedule_to_session", target_module=MODULE)
        for rel in rels:
            sess = store.find(MODULE, rel["target_id"])
            if sess and sess.get("关联源") == str(source_id) and sess.get("status") == "活跃":
                return dict(sess)
        # 查找该源下任意活跃会话
        for row in store.rows(MODULE):
            if row.get("关联源") == str(source_id) and row.get("status") == "活跃":
                return dict(row)
        return None
