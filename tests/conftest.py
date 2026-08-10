"""Make the tests import THIS checkout's `bridge`, not an installed one.

The package is normally installed editable (`pip install -e .`), which pins
`import bridge` to whichever checkout was installed from. In a git worktree
that is the *other* directory: tests then exercise the main checkout's code
while appearing to test the branch, and a real failure reads as a pass. Caught
2026-08-10 — new tests went green against unmodified code.

CI installs editable from its own checkout, so this resolves to the same files
there and changes nothing.
"""

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
