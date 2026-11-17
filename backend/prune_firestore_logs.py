"""Delete Firestore /logs documents older than four rolling days.

Dry-run is the default. To delete, pass --delete and confirm the project ID.
Documents with missing or unparseable received_at values are retained.
"""

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import firebase_admin
from firebase_admin import credentials, firestore


COLLECTION_NAME = "logs"
RECEIVED_AT_FIELD = "received_at"
KEEP_DAYS = 4
PAGE_SIZE = 100
DEFAULT_CREDENTIALS = (
    Path(__file__).resolve().parent
    / "secrets"
    / "firebase_service_account.json"
)


def parse_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Keep the most recent four days in Firestore's top-level /logs "
            "collection and identify older documents."
        )
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            or os.getenv("FIREBASE_CREDENTIALS_PATH")
            or DEFAULT_CREDENTIALS
        ),
        help="Service-account JSON path (defaults to environment or backend/secrets).",
    )
    parser.add_argument(
        "--project-id",
        default=os.getenv("FIREBASE_PROJECT_ID"),
        help="Firebase project ID (defaults to FIREBASE_PROJECT_ID or credential).",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete old documents; without this flag, only report them.",
    )
    parser.add_argument(
        "--confirm-project",
        help="Required with --delete; must exactly match the target project ID.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=PAGE_SIZE,
        help=f"Documents per Firestore request (1-500; default: {PAGE_SIZE}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.credentials.is_file():
        raise FileNotFoundError(
            f"Firebase service-account file not found: {args.credentials}"
        )

    service_account = credentials.Certificate(str(args.credentials))
    project_id = args.project_id or service_account.project_id
    if not project_id:
        raise ValueError(
            "Project ID is unavailable; pass --project-id or set FIREBASE_PROJECT_ID."
        )
    if args.delete and args.confirm_project != project_id:
        raise ValueError(
            "Deletion requires --confirm-project with the exact target project ID."
        )
    if not 1 <= args.page_size <= 500:
        raise ValueError("--page-size must be between 1 and 500.")

    app = firebase_admin.initialize_app(
        service_account,
        options={"projectId": project_id},
        name="prune_firestore_logs",
    )
    db = firestore.client(app=app)
    cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
    collection = db.collection(COLLECTION_NAME)

    print(f"Project: {project_id}")
    print(f"Collection: /{COLLECTION_NAME}")
    print(f"Keep records received at or after (UTC): {cutoff.isoformat()}")
    print("Mode: DELETE" if args.delete else "Mode: DRY RUN")

    scanned_candidates = 0
    old_count = 0
    unparseable_count = 0
    deleted_count = 0
    # Widen string comparisons for timezone offsets, then verify parsed UTC times.
    string_cutoff = (cutoff + timedelta(days=1)).isoformat()
    timestamp_queries = (
        collection.where(RECEIVED_AT_FIELD, ">=", "")
        .where(RECEIVED_AT_FIELD, "<", string_cutoff)
        .order_by(RECEIVED_AT_FIELD),
        collection.where(
            RECEIVED_AT_FIELD,
            ">=",
            datetime.min.replace(tzinfo=timezone.utc),
        )
        .where(RECEIVED_AT_FIELD, "<", cutoff)
        .order_by(RECEIVED_AT_FIELD),
    )

    for candidate_query in timestamp_queries:
        last_snapshot = None
        while True:
            query = candidate_query.limit(args.page_size)
            if last_snapshot is not None:
                query = query.start_after(last_snapshot)
            snapshots = list(query.stream())
            if not snapshots:
                break

            scanned_candidates += len(snapshots)
            batch = db.batch() if args.delete else None
            page_delete_count = 0
            for snapshot in snapshots:
                data = snapshot.to_dict() or {}
                received_at = parse_timestamp(data.get(RECEIVED_AT_FIELD))
                if received_at is None:
                    unparseable_count += 1
                    continue
                if received_at >= cutoff:
                    continue

                old_count += 1
                if batch is not None:
                    batch.delete(snapshot.reference)
                    page_delete_count += 1

            if batch is not None and page_delete_count:
                batch.commit()
                deleted_count += page_delete_count

            last_snapshot = snapshots[-1]
            if len(snapshots) < args.page_size:
                break

    print(f"Older timestamped candidates checked: {scanned_candidates}")
    print(f"Older than cutoff (verified): {old_count}")
    print(f"Unparseable timestamped candidates (retained): {unparseable_count}")
    if args.delete:
        print(f"Deleted: {deleted_count}")
    else:
        print("No documents deleted. Re-run with --delete and --confirm-project to delete.")


if __name__ == "__main__":
    main()
