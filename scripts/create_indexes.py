import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymongo.errors
from db import get_db


def main():
    db = get_db()

    indexes = [
        ("Urls", "link", True),
        ("new_daily_reports", "Source URL", True),
        ("new_daily_reports", "Report Title Arabic", False),
        ("Urls", "date", False),
    ]

    for collection, field, unique in indexes:
        try:
            db[collection].create_index(field, unique=unique)
            print(f"Created index on {collection}.{field} (unique={unique})")
        except pymongo.errors.OperationFailure as e:
            print(f"Index on {collection}.{field} already exists or failed: {e}")


if __name__ == "__main__":
    main()
