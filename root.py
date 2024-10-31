import os
from pathlib import Path


ROOT_PATH = Path(__file__).parent

def root() -> Path:
    p = ROOT_PATH.absolute().as_posix()
    os.chdir(p)
    return ROOT_PATH.absolute()