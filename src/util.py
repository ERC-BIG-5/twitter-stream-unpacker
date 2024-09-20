from pathlib import Path

from consts import logger


def consider_deletion(path: Path):
    delete_resp = input(f"Do you want to delete the file"
                        f"{path}? : y/ other key\n")
    if delete_resp == "y":
        logger.info(f"deleting: {path}")
        path.unlink()