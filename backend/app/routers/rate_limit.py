"""频次控制接口：维护限流策略，覆盖提交校验、标记生效、停用策略等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.rate_limit import RateLimitService

router = APIRouter(prefix="/api/rate_limit", tags=["频次控制"])

service = RateLimitService()

LIST_FIELDS = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、策略编号关键字与状态过滤频次控制列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="rate_limit.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条限流策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"限流策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条限流策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="限流策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条限流策略执行提交校验、标记生效、停用策略；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/acquire")
def acquire_token(entry_id: int) -> dict:
    """尝试获取请求令牌。"""
    return service.acquire_token(entry_id)


@router.post("/{entry_id}/release")
def release_token(entry_id: int) -> dict:
    """释放令牌回桶。"""
    return service.release_token(entry_id)


@router.post("/{entry_id}/quota-batches")
def submit_quota_batch(entry_id: int, payload: dict[str, object]) -> dict:
    """批量、多选、一次提交整组采集请求，返回逐条配额审计批次。

    每条请求只消耗一个稳定配额序号；同一幂等键重放复用首次结果，不重复扣减。
    """
    requests = payload.get("requests")
    if not isinstance(requests, list):
        raise HTTPException(status_code=400, detail="requests 必须是批量请求数组")
    result = service.submit_quota_batch(
        entry_id,
        requests,
        batch_id=str(payload["batch_id"]) if payload.get("batch_id") else None,
        retry_policy=str(payload.get("retry_policy", "reuse_first")),
        version_preference=str(payload.get("version_preference", "first")),
        idempotency_prefix=str(payload.get("idempotency_prefix", "")),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "配额审计批次生成失败"))
    return result


@router.get("/quota-batches/all")
def list_all_audit_batches() -> dict:
    """读取全部限流策略的配额审计批次（跨模块取数链路）。"""
    return {"items": service.list_audit_batches()}


@router.get("/{entry_id}/quota-batches")
def list_audit_batches(entry_id: int) -> dict:
    """读取单条限流策略的配额审计批次列表。"""
    return {"items": service.list_audit_batches(entry_id)}


@router.get("/{entry_id}/quota-batches/{batch_key:path}")
def get_audit_batch(entry_id: int, batch_key: str) -> dict:
    """读取单个配额审计批次，逐条查看允许、拒绝与剩余配额。"""
    batch = service.get_audit_batch(entry_id, batch_key)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"配额审计批次 {batch_key} 不存在")
    return batch
