"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime, timezone
import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        if response.status_code != 200:
            raise Exception(f"Failed to fetch items: {response.status_code} {response.text}")
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API."""
    all_logs = []
    params = {"limit": 500}
    if since:
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        params["since"] = since.isoformat()

    async with httpx.AsyncClient() as client:
        has_more = True
        while has_more:
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            if response.status_code != 200:
                raise Exception(f"Failed to fetch logs: {response.status_code} {response.text}")
            
            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)
            has_more = data.get("has_more", False)
            
            if has_more and logs:
                params["since"] = logs[-1]["submitted_at"]
                
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    new_count = 0
    lab_map = {} # lab_short_id -> ItemRecord

    # Process labs first
    for item_data in items:
        if item_data["type"] == "lab":
            title = item_data["title"]
            lab_short_id = item_data["lab"]
            
            statement = select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title == title)
            result = await session.exec(statement)
            lab_record = result.first()
            
            if not lab_record:
                lab_record = ItemRecord(type="lab", title=title)
                session.add(lab_record)
                new_count += 1
                await session.flush()
            
            lab_map[lab_short_id] = lab_record

    # Process tasks
    for item_data in items:
        if item_data["type"] == "task":
            title = item_data["title"]
            lab_short_id = item_data["lab"]
            parent_lab = lab_map.get(lab_short_id)
            
            if parent_lab:
                statement = select(ItemRecord).where(
                    ItemRecord.type == "task", 
                    ItemRecord.title == title, 
                    ItemRecord.parent_id == parent_lab.id
                )
                result = await session.exec(statement)
                task_record = result.first()
                
                if not task_record:
                    task_record = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
                    session.add(task_record)
                    new_count += 1
    
    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    new_count = 0
    
    # Build lookup: (lab_short_id, task_short_id) -> item title
    title_lookup = {}
    for item in items_catalog:
        key = (item["lab"], item.get("task"))
        title_lookup[key] = item["title"]

    for log in logs:
        # 1. Find or create Learner
        external_id = log["student_id"]
        statement = select(Learner).where(Learner.external_id == external_id)
        result = await session.exec(statement)
        learner = result.first()
        
        if not learner:
            learner = Learner(external_id=external_id, student_group=log.get("group", ""))
            session.add(learner)
            await session.flush()
            
        # 2. Find matching item
        item_title = title_lookup.get((log["lab"], log["task"]))
        if not item_title:
            continue
            
        # Query for item with this title. To avoid ambiguity between tasks of different labs,
        # we can also check the type if we know it from the lookup, 
        # but let's stick to title as requested, maybe adding type filter.
        is_task = log["task"] is not None
        item_type = "task" if is_task else "lab"
        
        statement = select(ItemRecord).where(ItemRecord.title == item_title, ItemRecord.type == item_type)
        result = await session.exec(statement)
        item_record = result.first()
        if not item_record:
            continue
            
        # 3. Idempotent check
        log_external_id = log["id"]
        statement = select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        result = await session.exec(statement)
        if result.first():
            continue
            
        # 4. Create InteractionLog
        submitted_at_str = log["submitted_at"]
        if submitted_at_str.endswith("Z"):
            submitted_at_str = submitted_at_str.replace("Z", "+00:00")
        submitted_at = datetime.fromisoformat(submitted_at_str)
        
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item_record.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at.replace(tzinfo=None)
        )
        session.add(interaction)
        new_count += 1
        
    await session.commit()
    return new_count


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    # Step 1: Items
    raw_items = await fetch_items()
    await load_items(raw_items, session)
    
    # Step 2: Last sync timestamp
    statement = select(func.max(InteractionLog.created_at))
    result = await session.exec(statement)
    last_timestamp = result.one()
    
    # Step 3: Logs
    new_logs = await fetch_logs(since=last_timestamp)
    new_interactions = await load_logs(new_logs, raw_items, session)
    
    # Total count
    statement = select(func.count(InteractionLog.id))
    result = await session.exec(statement)
    total_records = result.one()
    
    return {
        "new_records": new_interactions,
        "total_records": total_records
    }
