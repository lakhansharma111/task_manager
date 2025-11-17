Task Manager CLI (Python + SQLite)

A lightweight, single-file **command-line task manager** built using Python and SQLite.  
You can create, update, list, search, complete, and delete tasks directly from the terminal.

---

## 📌 Features

- Create tasks with:
  - Title
  - Description
  - Priority (`low`, `medium`, `high`)
  - Due date (`YYYY-MM-DD`)
- Update any field of existing tasks
- Mark tasks as **complete** or **incomplete**
- Search tasks by title or description
- Filter tasks by:
  - Status (`todo`, `in-progress`, `done`)
  - Priority
  - Due before / after
- Sort tasks by:
  - Created date
  - Due date
  - Priority
- View tasks in a clean formatted table
- SQLite storage (`tasks.db`)

---

## 🚀 Getting Started

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd task-manager-cli
