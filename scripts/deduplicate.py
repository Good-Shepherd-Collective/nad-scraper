import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_db


def deduplicate_collection(db, collection_name, field):
    """Remove duplicate documents based on a field, keeping the first occurrence."""
    pipeline = [
        {"$group": {"_id": f"${field}", "ids": {"$push": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]

    duplicates = list(db[collection_name].aggregate(pipeline))
    removed = 0

    for group in duplicates:
        # Keep the first _id, delete the rest
        ids_to_delete = group["ids"][1:]
        result = db[collection_name].delete_many({"_id": {"$in": ids_to_delete}})
        removed += result.deleted_count

    print(f"{collection_name}: removed {removed} duplicates (field: {field})")
    return removed


def main():
    db = get_db()

    total = 0
    total += deduplicate_collection(db, "Urls", "link")
    total += deduplicate_collection(db, "new_daily_reports", "Source URL")

    print(f"Total duplicates removed: {total}")


if __name__ == "__main__":
    main()
