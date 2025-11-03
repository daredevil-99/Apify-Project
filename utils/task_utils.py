# utils/task_utils.py
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from db_config import tasks

executor = ThreadPoolExecutor(max_workers=5)

def set_task_status(task_id: str, status: str, error: str = None):
    """Update in-memory task tracker safely."""
    tasks[task_id] = {
        "status": status,
        "error": error,
        "updated_at": datetime.utcnow().isoformat(),
    }

def run_in_background(func, *args):
    """Run blocking function in threadpool asynchronously."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(executor, lambda: func(*args))

async def run_and_track(task_id, func, *args):
    """Run a background task and automatically track its state."""
    try:
        set_task_status(task_id, "running")
        result = await run_in_background(func, *args)
        set_task_status(task_id, "completed")
        return result
    except Exception as e:
        set_task_status(task_id, "failed", error=str(e))
        raise
