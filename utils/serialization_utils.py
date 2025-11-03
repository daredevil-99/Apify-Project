# utils/serialization_utils.py
from bson import ObjectId
from datetime import datetime

def convert_objectid_to_str(data):
    """Recursively convert MongoDB ObjectId fields to strings."""
    if isinstance(data, dict):
        return {k: convert_objectid_to_str(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(i) for i in data]
    elif isinstance(data, ObjectId):
        return str(data)
    return data


def convert_datetime_to_str(obj):
    """Converts datetime objects to string for JSON-safe serialization."""
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    return obj

def serialize_crew_output(result):
    """Serialize nested CrewAI message output into safe dict for MongoDB."""
    import json
    try:
        if isinstance(result, str):
            return json.loads(result)
        elif isinstance(result, dict):
            return result
        else:
            return json.loads(str(result))
    except Exception:
        return {"message": str(result)}
