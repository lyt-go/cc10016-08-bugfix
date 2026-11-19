"""配额审计批次测试：批量请求配额校验、稳定序号、幂等重放与跨模块消费。

覆盖需求：
- 批量/多选/一次提交整组，逐条返回允许请求、拒绝请求与剩余配额；
- 每个批量请求只能消耗一个稳定配额序号，重复重放不再次扣减；
- 漏记/乱序序号会在审计批次中标出，避免与真实请求错位；
- 会话管理只消费已获得序号且结果为允许的审计项；
- 并发写入同一批次只保留一份结果；
- 存量历史版本批次结构迁移兼容；
- 令牌耗尽边界重试：沿用首次序号 vs 重新分配两种审计形态。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.rate_limit import RateLimitService
from app.services.session_mgr import SessionMgrService
from app.store import store


def setup_function() -> None:
    """每个测试函数前重置store状态。"""
    store._tables.clear()
    store._pipeline_states.clear()
    store._relations.clear()


def _create_rate_limit(capacity: int = 3, tokens: int = 10) -> int:
    svc = RateLimitService()
    entry, missing = svc.create_entry({
        "策略编号": f"RATE-T-{capacity}",
        "目标源": "SRC-TEST",
        "并发上限": str(tokens),
        "日请求总量": str(capacity),
    })
    assert not missing and entry is not None
    svc.run_action(entry["id"], "提交校验")
    svc.run_action(entry["id"], "标记生效")
    return int(entry["id"])


def _create_session() -> int:
    svc = SessionMgrService()
    entry, missing = svc.create_entry({"会话编号": "SESS-T-01", "关联源": "SRC-TEST"})
    assert not missing and entry is not None
    return int(entry["id"])


def test_batch_over_balance_then_replay_last_item() -> None:
    """提交超过余额的批量请求，再重放最后一条，核对逐条结果与最终余额。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=3)
    requests = [
        {"url": f"https://example.com/item/{i}", "idempotency_key": f"k-{i}", "seq": i + 1}
        for i in range(5)
    ]

    batch = svc.submit_quota_batch(rate_id, requests, batch_id="b-overflow")
    assert batch["ok"] is True
    items = batch["audit_items"]
    assert len(items) == 5
    # 前三条允许，后两条因日配额耗尽拒绝，逐条可追踪。
    assert [item["allowed"] for item in items] == [True, True, True, False, False]
    assert [item["decision"] for item in items] == [
        "允许请求", "允许请求", "允许请求", "拒绝请求", "拒绝请求"]
    # 每条都拿到稳定配额序号，拒绝项也不漏记，审计行与真实请求不错位。
    assert [item["quota_seq"] for item in items] == [1, 2, 3, 4, 5]
    assert batch["allowed_count"] == 3
    assert batch["denied_count"] == 2
    assert batch["deducted_count"] == 3
    assert batch["remaining_quota"] == 0
    # 剩余配额逐条递减后停在 0。
    assert [item["remaining_quota"] for item in items] == [2, 1, 0, 0, 0]

    # 重放最后一条（同样的幂等键）：复用首次序号与拒绝结论，不产生新扣减。
    replay = svc.submit_quota_batch(
        rate_id, [requests[-1]], batch_id="b-replay-last")
    replay_item = replay["audit_items"][0]
    assert replay_item["replay"] is True
    assert replay_item["allowed"] is False
    assert replay_item["quota_seq"] == 5
    assert replay_item["replay_count"] == 1
    assert replay["deducted_count"] == 0
    assert replay["remaining_quota"] == 0

    # 令牌桶水位未因重放变化。
    states = store.pipeline_state("rate_limit_states")
    assert states[str(rate_id)]["daily_used"] == 3


def test_each_request_consumes_one_stable_seq_and_replay_reuses_it() -> None:
    """同一幂等键重放沿用首次序号，只产生一份有效扣减。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=10)
    payload = [{"url": "https://a.test", "idempotency_key": "dup", "seq": 1}]

    first = svc.submit_quota_batch(rate_id, payload, batch_id="b1")
    second = svc.submit_quota_batch(rate_id, payload, batch_id="b2")
    third = svc.submit_quota_batch(rate_id, payload, batch_id="b3")

    assert first["audit_items"][0]["quota_seq"] == 1
    assert second["audit_items"][0]["quota_seq"] == 1
    assert third["audit_items"][0]["quota_seq"] == 1
    assert third["audit_items"][0]["replay_count"] == 2
    assert sum(b["deducted_count"] for b in svc.list_audit_batches(rate_id)) == 1
    states = store.pipeline_state("rate_limit_states")
    assert states[str(rate_id)]["daily_used"] == 1


def test_missing_or_disordered_seq_is_flagged_not_silently_realiged() -> None:
    """漏记/乱序序号必须在审计批次中标出，审计行仍按提交顺序逐条返回。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=10)
    requests = [
        {"url": "u1", "idempotency_key": "o1", "seq": 1},
        {"url": "u2", "idempotency_key": "o2", "seq": 3},  # 乱序：期望 2
        {"url": "u3", "idempotency_key": "o3"},            # 漏记：回退为顺序位 3
    ]
    batch = svc.submit_quota_batch(rate_id, requests, batch_id="b-order")
    assert batch["order_violation"] is True
    items = batch["audit_items"]
    assert [item["request_index"] for item in items] == [1, 2, 3]
    assert items[1]["submitted_order"] == 3
    assert "顺序错位" in items[1]["reason"]
    assert items[2]["submitted_order"] == 3
    assert items[2]["quota_seq"] == 3  # 序号水位不受乱序影响，逐条连续分配


def test_session_consumes_only_allowed_seq_assigned_items() -> None:
    """会话管理消费配额审计批次：拒绝、重放、重复消费都不写会话日志。"""
    rate_svc = RateLimitService()
    sess_svc = SessionMgrService()
    rate_id = _create_rate_limit(capacity=2)
    sess_id = _create_session()
    requests = [
        {"url": f"u-{i}", "idempotency_key": f"s-{i}", "seq": i + 1} for i in range(4)
    ]
    batch = rate_svc.submit_quota_batch(rate_id, requests, batch_id="b-consume")
    assert batch["allowed_count"] == 2 and batch["denied_count"] == 2

    consumed = sess_svc.consume_quota_batch(
        sess_id, rate_id, batch_id="b-consume", audit_items=batch["audit_items"])
    assert consumed["written"] == 2
    assert consumed["skipped_denied"] == 2
    assert consumed["request_count"] == 2

    logs = store.pipeline_state("session_request_logs")[str(sess_id)]
    assert {log["quota_seq"] for log in logs} == {1, 2}
    assert all(log["decision"] == "允许请求" for log in logs)
    # 列表页与详情页同源取数。
    detail = sess_svc.get_entry(sess_id)
    assert detail["_request_log"] == logs

    # 幂等重放行不会再次写日志。
    replay = rate_svc.submit_quota_batch(
        rate_id, [requests[0]], batch_id="b-replay")
    consumed_replay = sess_svc.consume_quota_batch(
        sess_id, rate_id, batch_id="b-replay", audit_items=replay["audit_items"])
    assert consumed_replay["written"] == 0
    assert consumed_replay["skipped_replay"] == 1

    # 同一批次重复消费只保留一份会话日志。
    again = sess_svc.consume_quota_batch(
        sess_id, rate_id, batch_id="b-consume", audit_items=batch["audit_items"])
    assert again["written"] == 0
    assert again["skipped_duplicate"] == 2
    assert len(store.pipeline_state("session_request_logs")[str(sess_id)]) == 2


def test_concurrent_batch_writes_keep_single_result() -> None:
    """并发写入同一配额审计批次只保留一份结果。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=100)
    requests = [{"url": f"c-{i}", "idempotency_key": f"c-{i}", "seq": i + 1} for i in range(10)]
    results: list[dict] = []

    def worker() -> None:
        results.append(svc.submit_quota_batch(rate_id, requests, batch_id="b-concurrent"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 8
    stored = svc.list_audit_batches(rate_id)
    same_batch = [b for b in stored if b["batch_id"] == "b-concurrent"]
    assert len(same_batch) == 1  # 只落一份审计批次
    # 只有首批真正扣减，其余并发请求命中同一份结果。
    states = store.pipeline_state("rate_limit_states")
    assert states[str(rate_id)]["daily_used"] == 10
    assert all(item["quota_seq"] <= 10 for item in same_batch[0]["audit_items"])


def test_legacy_audit_batches_are_migrated() -> None:
    """存量历史版本批次结构可被读取并自动补齐字段。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=5)
    legacy = {
        str(rate_id): [
            {
                "batch_id": "legacy-1",
                "rate_limit_id": rate_id,
                "audit_items": [
                    {"audit_id": "old-1", "quota_seq": 1, "allowed": True,
                     "idempotency_key": "legacy-k", "decision": "允许请求"},
                ],
            }
        ]
    }
    store.set_pipeline_state("quota_audit_batches", legacy)

    batches = svc.list_audit_batches(rate_id)
    assert len(batches) == 1
    assert batches[0]["schema_version"] == 1  # 历史版本号保留
    assert batches[0]["retry_policy"] == "reuse_first"
    assert batches[0]["audit_items"][0]["replay"] is False
    assert svc.get_audit_batch(rate_id, "legacy-1")["batch_id"] == "legacy-1"


def test_retry_at_exhausted_boundary_reuse_first_keeps_single_deduction() -> None:
    """耗尽边界重试：默认沿用首次序号，审计显示一次扣减 + 一次幂等重放。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=1)

    first = svc.submit_quota_batch(
        rate_id, [{"url": "edge", "idempotency_key": "edge-k", "version": "v1", "seq": 1}],
        batch_id="edge-1")
    assert first["audit_items"][0]["allowed"] is True
    assert first["remaining_quota"] == 0

    # 令牌已耗尽后重试同一幂等键（新版本号），默认仍复用首次序号与允许结论。
    retry = svc.submit_quota_batch(
        rate_id, [{"url": "edge", "idempotency_key": "edge-k", "version": "v2", "seq": 1}],
        batch_id="edge-2")
    item = retry["audit_items"][0]
    assert item["replay"] is True
    assert item["allowed"] is True
    assert item["quota_seq"] == 1
    assert item["kept_version"] == "v1"
    states = store.pipeline_state("rate_limit_states")
    assert states[str(rate_id)]["daily_used"] == 1  # 没有二次扣减


def test_retry_at_exhausted_boundary_reassign_records_two_audit_rows() -> None:
    """耗尽边界重试：重新分配策略下旧序号拒绝、新序号成功，两条独立审计行。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=2)

    svc.submit_quota_batch(
        rate_id, [{"url": "edge", "idempotency_key": "edge-k", "version": "v1", "seq": 1}],
        batch_id="edge-a")
    # 再消耗一个请求把日配额打满，制造耗尽边界。
    svc.submit_quota_batch(
        rate_id, [{"url": "other", "idempotency_key": "other-k", "seq": 1}],
        batch_id="edge-b")
    states = store.pipeline_state("rate_limit_states")
    assert states[str(rate_id)]["daily_used"] == 2

    # 重新分配：旧序号先回滚扣减并保留为失败审计项，新序号重新参与配额判定。
    retry = svc.submit_quota_batch(
        rate_id,
        [{"url": "edge", "idempotency_key": "edge-k", "version": "v2", "seq": 1}],
        batch_id="edge-c", retry_policy="reassign", version_preference="latest")
    items = retry["audit_items"]
    assert len(items) == 2
    old_item, new_item = items
    assert old_item["allowed"] is False
    assert old_item["quota_seq"] == 1
    assert "旧序号保留为失败审计项" in old_item["reason"]
    assert new_item["allowed"] is True
    assert new_item["quota_seq"] == 3
    # 同一幂等键最终仍只有一份有效扣减。
    states = store.pipeline_state("rate_limit_states")
    assert states[str(rate_id)]["daily_used"] == 2
    assert retry["deducted_count"] == 1


def test_idempotency_key_auto_derived_is_stable() -> None:
    """未显式给幂等键时，相同请求内容生成稳定键，重放仍不二次扣减。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=5)
    request = {"url": "https://stable.test/x", "seq": 1}
    first = svc.submit_quota_batch(rate_id, [request], batch_id="auto-1")
    second = svc.submit_quota_batch(rate_id, [request], batch_id="auto-2")
    assert first["audit_items"][0]["idempotency_key"] == second["audit_items"][0]["idempotency_key"]
    assert second["audit_items"][0]["replay"] is True
    assert store.pipeline_state("rate_limit_states")[str(rate_id)]["daily_used"] == 1


def test_reassign_when_new_candidate_still_denied_keeps_zero_deduction() -> None:
    """重新分配后新候选仍被拒绝：旧序号失败、新序号失败，零有效扣减且索引不回退。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=1, tokens=10)

    # 另一个幂等键先耗尽唯一的日配额。
    svc.submit_quota_batch(
        rate_id, [{"url": "other", "idempotency_key": "other-k", "version": "v1", "seq": 1}],
        batch_id="edge-fill")
    # edge-k 首次提交即在耗尽边界被拒。
    first = svc.submit_quota_batch(
        rate_id, [{"url": "edge", "idempotency_key": "edge-k", "version": "v1", "seq": 1}],
        batch_id="edge-v1")
    assert first["audit_items"][0]["allowed"] is False

    # 重新分配：旧序号无扣减可回滚，新候选在耗尽边界上仍被拒。
    retry = svc.submit_quota_batch(
        rate_id,
        [{"url": "edge", "idempotency_key": "edge-k", "version": "v2", "seq": 1}],
        batch_id="edge-v2", retry_policy="reassign", version_preference="latest")
    items = retry["audit_items"]
    assert len(items) == 2
    assert all(item["allowed"] is False for item in items)
    assert items[0]["quota_seq"] == 2 and items[1]["quota_seq"] == 3
    # 两条审计行都计拒绝，计数器与逐条行数一致。
    assert retry["denied_count"] == 2
    assert retry["allowed_count"] == 0
    assert retry["deducted_count"] == 0
    assert retry["allowed_count"] + retry["denied_count"] + retry["replay_count"] == retry["total"]
    # 后续默认重放拿到的是最新（拒绝）结论，不会回退到旧序号。
    replay = svc.submit_quota_batch(
        rate_id, [{"url": "edge", "idempotency_key": "edge-k", "version": "v2", "seq": 1}],
        batch_id="edge-replay")
    assert replay["audit_items"][0]["allowed"] is False
    assert replay["audit_items"][0]["replay"] is True
    assert replay["audit_items"][0]["quota_seq"] == 3
    # 全程只有 other-k 一次有效扣减。
    assert store.pipeline_state("rate_limit_states")[str(rate_id)]["daily_used"] == 1


def test_audit_counter_sums_match_item_rows() -> None:
    """每个批次的允许+拒绝+重放计数必须与逐条审计行数一致。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=2)
    requests = [
        {"url": f"u-{i}", "idempotency_key": f"cnt-{i}", "seq": i + 1} for i in range(4)
    ]
    batch = svc.submit_quota_batch(rate_id, requests, batch_id="b-cnt")
    assert batch["allowed_count"] + batch["denied_count"] + batch["replay_count"] == batch["total"]
    replay = svc.submit_quota_batch(rate_id, requests[:2], batch_id="b-cnt-r")
    assert replay["replay_count"] == 2
    assert replay["allowed_count"] + replay["denied_count"] + replay["replay_count"] == replay["total"]


def test_inactive_policy_rejects_whole_batch_with_seq_recorded() -> None:
    """策略未生效时整组拒绝，但稳定序号仍逐条分配，审计不错位。"""
    svc = RateLimitService()
    rate_id = _create_rate_limit(capacity=5)
    svc.run_action(rate_id, "停用策略")
    batch = svc.submit_quota_batch(
        rate_id,
        [{"url": f"u-{i}", "idempotency_key": f"d-{i}", "seq": i + 1} for i in range(3)],
        batch_id="b-disabled")
    assert all(item["allowed"] is False for item in batch["audit_items"])
    assert [item["quota_seq"] for item in batch["audit_items"]] == [1, 2, 3]
    assert batch["deducted_count"] == 0
