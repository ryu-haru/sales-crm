"""
Vercel serverless entry point.

Vercel sets cwd to the project root for Python functions, but to be safe
we resolve paths relative to this file's parent (which is the `api/`
directory, one level below project root).
"""
import sys
from pathlib import Path

# Ensure the project root (parent of api/) is on sys.path so that
# `main` and `database` are importable.
_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# Patch working directory so Jinja2 / StaticFiles can find templates/ and static/
import os
os.chdir(str(_root))

# Now import the FastAPI app from main.py
from main import app  # noqa: E402  (import after sys.path/chdir manipulation)

# Vercel's Python runtime looks for a symbol named `handler` (ASGI/WSGI).
# Exporting `app` directly also works with recent Vercel Python runtimes.
handler = app
