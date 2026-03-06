"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
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
            raise Exception(f"Failed to fetch items: {response.status_code}")
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API."""
    all_logs = []
    has_more = True
    current_since = since.isoformat() if since else None

    async with httpx.AsyncClient() as client:
        while has_more:
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            if response.status_code != 200:
                raise Exception(f"Failed to fetch logs: {response.status_code}")

            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)
            has_more = data.get("has_more", False)

            if logs and has_more:
                current_since = logs[-1]["submitted_at"]

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    new_count = 0
    lab_map = {}  # lab_short_id -> ItemRecord

    # Process labs
    for item in [i for i in items if i["type"] == "lab"]:
        lab_id = item["lab"]
        title = item["title"]

        statement = select(ItemRecord).where(
            ItemRecord.type == "lab", ItemRecord.title == title
        )
        result = await session.exec(statement)
        db_item = result.first()

        if not db_item:
            db_item = ItemRecord(type="lab", title=title)
            session.add(db_item)
            new_count += 1
            await session.flush()
        
        lab_map[lab_id] = db_item

    # Process tasks
    for item in [i for i in items if i["type"] == "task"]:
        lab_id = item["lab"]
        title = item["title"]
        parent_lab = lab_map.get(lab_id)

        if not parent_lab:
            continue

        statement = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == title,
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(statement)
        db_item = result.first()

        if not db_item:
            db_item = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(db_item)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    new_interactions = 0

    # Build lookup from (lab_id, task_id) to title
    lookup = {}
    for item in items_catalog:
        key = (item["lab"], item["task"])
        lookup[key] = item["title"]

    for log in logs:
        # 1. Find or create Learner
        external_id = log["student_id"]
        statement = select(Learner).where(Learner.external_id == external_id)
        result = await session.exec(statement)
        learner = result.first()

        if not learner:
            learner = Learner(external_id=external_id, student_group=log["group"])
            session.add(learner)
            await session.flush()

        # 2. Find matching ItemRecord
        title = lookup.get((log["lab"], log["task"]))
        if not title:
            continue

        statement = select(ItemRecord).where(ItemRecord.title == title)
        result = await session.exec(statement)
        db_item = result.first()

        if not db_item:
            continue

        # 3. Check for existing InteractionLog (idempotency)
        log_external_id = log["id"]
        statement = select(InteractionLog).where(
            InteractionLog.external_id == log_external_id
        )
        result = await session.exec(statement)
        if result.first():
            continue

        # 4. Create InteractionLog
        new_log = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=db_item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00")).replace(tzinfo=None),
        )
        session.add(new_log)
        new_interactions += 1

    await session.commit()
    return new_interactions


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    # Step 1: Items
    raw_items = await fetch_items()
    await load_items(raw_items, session)

    # Step 2: Determine last synced timestamp
    statement = select(func.max(InteractionLog.created_at))
    result = await session.exec(statement)
    last_sync = result.one()

    # Step 3: Logs
    logs = await fetch_logs(since=last_sync)
    new_interactions = await load_logs(logs, raw_items, session)

    # Total records
    statement = select(func.count(InteractionLog.id))
    result = await session.exec(statement)
    total_records = result.one()

    return {
        "new_records": new_interactions,
        "total_records": total_records
    }
