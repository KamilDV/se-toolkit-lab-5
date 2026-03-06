"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, case, cast, Date
from sqlmodel import select, col
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


async def get_lab_task_ids(lab: str, session: AsyncSession) -> list[int]:
    """Helper to get all task IDs for a given lab string."""
    lab_search = lab.replace("-", " ").title() # e.g. lab-04 -> Lab 04
    
    # 1. Find the lab item
    statement = select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.contains(lab_search))
    result = await session.exec(statement)
    lab_item = result.first()
    if not lab_item:
        return []
        
    # 2. Find all tasks for this lab
    statement = select(ItemRecord.id).where(ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id)
    result = await session.exec(statement)
    return list(result.all())


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    task_ids = await get_lab_task_ids(lab, session)
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
        
    # Query interactions for these tasks that have a score
    # Use CASE WHEN to bucket scores
    statement = select(
        case(
            (InteractionLog.score <= 25, "0-25"),
            (InteractionLog.score <= 50, "26-50"),
            (InteractionLog.score <= 75, "51-75"),
            else_="76-100"
        ).label("bucket"),
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.is_not(None)
    ).group_by("bucket")
    
    result = await session.exec(statement)
    counts = {row.bucket: row.count for row in result.all()}
    
    # Ensure all buckets are present
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [{"bucket": b, "count": counts.get(b, 0)} for b in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_search = lab.replace("-", " ").title()
    
    # Find lab item
    statement = select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.contains(lab_search))
    result = await session.exec(statement)
    lab_item = result.first()
    if not lab_item:
        return []
        
    # Query tasks and their interactions
    statement = select(
        ItemRecord.title.label("task"),
        func.round(cast(func.avg(InteractionLog.score), func.Numeric), 1).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).join(
        InteractionLog, ItemRecord.id == InteractionLog.item_id, isouter=True
    ).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    ).group_by(
        ItemRecord.id, ItemRecord.title
    ).order_by(
        ItemRecord.title
    )
    
    result = await session.exec(statement)
    return [
        {
            "task": row.task, 
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0, 
            "attempts": row.attempts
        } 
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    task_ids = await get_lab_task_ids(lab, session)
    if not task_ids:
        return []
        
    statement = select(
        cast(InteractionLog.created_at, Date).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        "date"
    ).order_by(
        "date"
    )
    
    result = await session.exec(statement)
    return [{"date": str(row.date), "submissions": row.submissions} for row in result.all()]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    task_ids = await get_lab_task_ids(lab, session)
    if not task_ids:
        return []
        
    statement = select(
        Learner.student_group.label("group"),
        func.round(cast(func.avg(InteractionLog.score), func.Numeric), 1).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, Learner.id == InteractionLog.learner_id
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.exec(statement)
    return [
        {
            "group": row.group, 
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0, 
            "students": row.students
        } 
        for row in result.all()
    ]
