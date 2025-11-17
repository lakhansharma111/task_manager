

from __future__ import annotations
import argparse
import sqlite3
import sys
from datetime import datetime
from typing import List, Optional, Tuple

DB_FILE = "tasks.db"

# ----- Constants / Helpers ----- #
ALLOWED_PRIORITIES = ("low", "medium", "high")
ALLOWED_STATUSES = ("todo", "in-progress", "done")
DATE_FORMAT = "%Y-%m-%d"  # due date format


def get_db_connection(db_file: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create table if not exists."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            created_at TEXT NOT NULL,
            due_date TEXT,
            completed_at TEXT
        )
        """
    )
    conn.commit()


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


# ----- Validation ----- #
def validate_title(title: str) -> None:
    if not title or not title.strip():
        raise ValueError("Title must not be empty.")


def validate_priority(priority: str) -> str:
    p = priority.strip().lower()
    if p not in ALLOWED_PRIORITIES:
        raise ValueError(f"Priority must be one of {ALLOWED_PRIORITIES}.")
    return p


def validate_status(status: str) -> str:
    s = status.strip().lower()
    if s not in ALLOWED_STATUSES:
        raise ValueError(f"Status must be one of {ALLOWED_STATUSES}.")
    return s


def validate_due_date(date_str: Optional[str]) -> Optional[str]:
    if date_str is None:
        return None
    date_str = date_str.strip()
    if date_str == "":
        return None
    try:
        dt = datetime.strptime(date_str, DATE_FORMAT)
        return dt.strftime(DATE_FORMAT)
    except ValueError:
        raise ValueError(f"Due date must be in {DATE_FORMAT} format (e.g. 2025-11-20).")


# ----- CRUD operations ----- #
def create_task(
    conn: sqlite3.Connection,
    title: str,
    description: str = "",
    priority: str = "medium",
    due_date: Optional[str] = None,
) -> int:
    validate_title(title)
    p = validate_priority(priority)
    dd = validate_due_date(due_date)

    created_at = _now_iso()
    status = "todo"
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tasks (title, description, status, priority, created_at, due_date, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (title.strip(), description.strip(), status, p, created_at, dd, None),
    )
    conn.commit()
    task_id = cur.lastrowid
    print(f"Created task #{task_id}")
    return task_id


def get_task(conn: sqlite3.Connection, task_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
    row = cur.fetchone()
    return row


def update_task(
    conn: sqlite3.Connection,
    task_id: int,
    title: Optional[str] = None,
    description: Optional[str] = None,
    status: Optional[str] = None,
    priority: Optional[str] = None,
    due_date: Optional[str] = None,
) -> bool:
    task = get_task(conn, task_id)
    if not task:
        raise LookupError(f"Task with id {task_id} not found.")

    updates = {}
    if title is not None:
        validate_title(title)
        updates["title"] = title.strip()
    if description is not None:
        updates["description"] = description.strip()
    if status is not None:
        updates["status"] = validate_status(status)
        if updates["status"] == "done" and not task["completed_at"]:
            updates["completed_at"] = _now_iso()
        elif updates["status"] != "done" and task["completed_at"]:
            updates["completed_at"] = None
    if priority is not None:
        updates["priority"] = validate_priority(priority)
    if due_date is not None:
        updates["due_date"] = validate_due_date(due_date)

    if not updates:
        print("No updates provided.")
        return False

    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    params = list(updates.values()) + [task_id]
    cur = conn.cursor()
    cur.execute(f"UPDATE tasks SET {set_clause} WHERE id = ?", params)
    conn.commit()
    print(f"Updated task #{task_id}")
    return True


def delete_task(conn: sqlite3.Connection, task_id: int) -> bool:
    if not get_task(conn, task_id):
        raise LookupError(f"Task with id {task_id} not found.")
    cur = conn.cursor()
    cur.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()
    print(f"Deleted task #{task_id}")
    return True


# ----- Mark complete/incomplete ----- #
def mark_complete(conn: sqlite3.Connection, task_id: int) -> bool:
    task = get_task(conn, task_id)
    if not task:
        raise LookupError(f"Task {task_id} not found.")
    if task["status"] == "done":
        print(f"Task #{task_id} is already done.")
        return False
    cur = conn.cursor()
    cur.execute(
        "UPDATE tasks SET status = ?, completed_at = ? WHERE id = ?",
        ("done", _now_iso(), task_id),
    )
    conn.commit()
    print(f"Marked task #{task_id} as complete.")
    return True


def mark_incomplete(conn: sqlite3.Connection, task_id: int) -> bool:
    task = get_task(conn, task_id)
    if not task:
        raise LookupError(f"Task {task_id} not found.")
    if task["status"] != "done":
        print(f"Task #{task_id} is not done.")
        return False
    cur = conn.cursor()
    cur.execute(
        "UPDATE tasks SET status = ?, completed_at = ? WHERE id = ?",
        ("todo", None, task_id),
    )
    conn.commit()
    print(f"Marked task #{task_id} as incomplete.")
    return True


# ----- Listing / filtering ----- #
def _build_list_query(
    status: Optional[str],
    priority: Optional[str],
    due_before: Optional[str],
    due_after: Optional[str],
    search: Optional[str],
    order_by: Optional[str],
) -> Tuple[str, List]:
    where_clauses = []
    params: List = []
    if status:
        where_clauses.append("status = ?")
        params.append(status)
    if priority:
        where_clauses.append("priority = ?")
        params.append(priority)
    if due_before:
        where_clauses.append("due_date <= ?")
        params.append(due_before)
    if due_after:
        where_clauses.append("due_date >= ?")
        params.append(due_after)
    if search:
        where_clauses.append("(title LIKE ? OR description LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like])

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    order_sql = ""
    if order_by == "due":
        order_sql = "ORDER BY due_date IS NULL, due_date"
    elif order_by == "priority":
        # custom priority order high -> medium -> low
        order_sql = "ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END"
    else:
        order_sql = "ORDER BY created_at DESC"

    sql = f"SELECT * FROM tasks {where_sql} {order_sql}"
    return sql, params


def list_tasks(
    conn: sqlite3.Connection,
    status: Optional[str] = None,
    priority: Optional[str] = None,
    due_before: Optional[str] = None,
    due_after: Optional[str] = None,
    search: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[sqlite3.Row]:
    if status:
        status = validate_status(status)
    if priority:
        priority = validate_priority(priority)
    if due_before:
        due_before = validate_due_date(due_before)
    if due_after:
        due_after = validate_due_date(due_after)

    sql, params = _build_list_query(status, priority, due_before, due_after, search, order_by)
    if limit and isinstance(limit, int) and limit > 0:
        sql += f" LIMIT {limit}"
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    return rows


# ----- Presentation ----- #
def format_task_row(row: sqlite3.Row) -> str:
    return (
        f"[#{row['id']}] {row['title']} "
        f"(status={row['status']}, priority={row['priority']}, "
        f"due={row['due_date'] or '—'}, created={row['created_at']})"
    )


def print_task_detail(row: sqlite3.Row) -> None:
    print(f"ID: {row['id']}")
    print(f"Title: {row['title']}")
    print(f"Description: {row['description'] or '—'}")
    print(f"Status: {row['status']}")
    print(f"Priority: {row['priority']}")
    print(f"Created at: {row['created_at']}")
    print(f"Due date: {row['due_date'] or '—'}")
    print(f"Completed at: {row['completed_at'] or '—'}")


def print_task_list(rows: List[sqlite3.Row]) -> None:
    if not rows:
        print("No tasks found.")
        return
    # simple table
    print("-" * 80)
    print(f"{'ID':<5} {'TITLE':<30} {'STATUS':<12} {'PRIORITY':<8} {'DUE':<10}")
    print("-" * 80)
    for r in rows:
        due = r["due_date"] or "—"
        title = r["title"]
        if len(title) > 28:
            title = title[:25] + "..."
        print(f"{r['id']:<5} {title:<30} {r['status']:<12} {r['priority']:<8} {due:<10}")
    print("-" * 80)
    print(f"Total: {len(rows)}")


# ----- CLI wiring (argparse) ----- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Task Manager CLI (single-file, SQLite)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # add
    add = sub.add_parser("add", help="Create a new task")
    add.add_argument("--title", required=True, help="Title of the task")
    add.add_argument("--desc", default="", help="Description")
    add.add_argument("--priority", default="medium", choices=ALLOWED_PRIORITIES, help="Priority")
    add.add_argument("--due", default=None, help=f"Due date in {DATE_FORMAT}")

    # get
    get = sub.add_parser("get", help="Get details of a task")
    get.add_argument("id", type=int)

    # list
    lst = sub.add_parser("list", help="List tasks with optional filters")
    lst.add_argument("--status", choices=ALLOWED_STATUSES)
    lst.add_argument("--priority", choices=ALLOWED_PRIORITIES)
    lst.add_argument("--due-before", dest="due_before", help=f"Due before {DATE_FORMAT}")
    lst.add_argument("--due-after", dest="due_after", help=f"Due after {DATE_FORMAT}")
    lst.add_argument("--search", help="Search in title and description")
    lst.add_argument("--order-by", choices=("created", "due", "priority"), default="created")
    lst.add_argument("--limit", type=int, default=None)

    # update
    upd = sub.add_parser("update", help="Update task fields")
    upd.add_argument("id", type=int)
    upd.add_argument("--title", help="New title")
    upd.add_argument("--desc", dest="description", help="New description")
    upd.add_argument("--status", choices=ALLOWED_STATUSES, help="New status")
    upd.add_argument("--priority", choices=ALLOWED_PRIORITIES, help="New priority")
    upd.add_argument("--due", dest="due_date", help=f"Due date in {DATE_FORMAT}")

    # complete
    comp = sub.add_parser("complete", help="Mark task as complete")
    comp.add_argument("id", type=int)

    # incomplete
    incom = sub.add_parser("incomplete", help="Mark task as incomplete (undo complete)")
    incom.add_argument("id", type=int)

    # delete
    delete = sub.add_parser("delete", help="Delete a task")
    delete.add_argument("id", type=int)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    conn = get_db_connection()
    init_db(conn)

    try:
        if args.cmd == "add":
            create_task(conn, args.title, args.desc or "", args.priority, args.due)
            return 0

        if args.cmd == "get":
            row = get_task(conn, args.id)
            if not row:
                print(f"Task {args.id} not found.")
                return 2
            print_task_detail(row)
            return 0

        if args.cmd == "list":
            order_by_map = {"created": None, "due": "due", "priority": "priority"}
            order_by = order_by_map.get(args.order_by)
            rows = list_tasks(
                conn,
                status=args.status,
                priority=args.priority,
                due_before=args.due_before,
                due_after=args.due_after,
                search=args.search,
                order_by=order_by,
                limit=args.limit,
            )
            print_task_list(rows)
            return 0

        if args.cmd == "update":
            updated = update_task(
                conn,
                args.id,
                title=args.title,
                description=args.description,
                status=args.status,
                priority=args.priority,
                due_date=args.due_date,
            )
            return 0 if updated else 1

        if args.cmd == "complete":
            mark_complete(conn, args.id)
            return 0

        if args.cmd == "incomplete":
            mark_incomplete(conn, args.id)
            return 0

        if args.cmd == "delete":
            delete_task(conn, args.id)
            return 0

    except ValueError as ve:
        print(f"Validation error: {ve}")
        return 3
    except LookupError as le:
        print(f"Not found error: {le}")
        return 4
    except sqlite3.Error as se:
        print(f"Database error: {se}")
        return 5
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
