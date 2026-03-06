"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


async def get_lab_and_tasks(lab_id: str, session: AsyncSession) -> tuple[int, list[int]]:
    """Helper to find lab and task IDs for a given lab identifier."""
    # Convert "lab-04" -> "Lab 04"
    lab_part = lab_id.split("-")[-1]
    lab_title_part = f"Lab {lab_part}"
    
    # Find lab item
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(f"%{lab_title_part}%")
    )
    result = await session.exec(statement)
    lab_item = result.first()
    
    if not lab_item:
        return None, []
        
    # Find child tasks
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.exec(statement)
    task_ids = result.all()
    
    return lab_item.id, list(task_ids)


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    lab_id, task_ids = await get_lab_and_tasks(lab, session)
    if not lab_id:
        return []

    # Score buckets using CASE WHEN
    statement = (
        select(
            case(
                (InteractionLog.score <= 25, "0-25"),
                (InteractionLog.score <= 50, "26-50"),
                (InteractionLog.score <= 75, "51-75"),
                else_="76-100",
            ).label("bucket"),
            func.count(InteractionLog.id).label("count"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by("bucket")
    )
    
    result = await session.exec(statement)
    counts = {r.bucket: r.count for r in result.all()}
    
    # Ensure all four buckets are present
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [{"bucket": b, "count": counts.get(b, 0)} for b in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_id, _ = await get_lab_and_tasks(lab, session)
    if not lab_id:
        return []

    # Join tasks with their interactions
    statement = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_id)
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    
    result = await session.exec(statement)
    return [
        {"task": r.task, "avg_score": float(r.avg_score or 0), "attempts": r.attempts}
        for r in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    _, task_ids = await get_lab_and_tasks(lab, session)
    if not task_ids:
        return []

    # Group by date
    statement = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )
    
    result = await session.exec(statement)
    return [
        {"date": str(r.date), "submissions": r.submissions}
        for r in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    _, task_ids = await get_lab_and_tasks(lab, session)
    if not task_ids:
        return []

    # Join interactions with learners
    statement = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    
    result = await session.exec(statement)
    return [
        {"group": r.group, "avg_score": float(r.avg_score or 0), "students": r.students}
        for r in result.all()
    ]
