from pathlib import Path

from debugpy._vendored._util import cwd

ROOT_PATH = Path(__file__).parent

def root() -> Path:
    return cwd(ROOT_PATH)