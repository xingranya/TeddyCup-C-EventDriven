from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.event_ingest import main  # noqa: E402
from pipeline.utils import configure_logging  # noqa: E402


if __name__ == "__main__":
    configure_logging(logging.INFO)
    raise SystemExit(main(sys.argv[1:]))
