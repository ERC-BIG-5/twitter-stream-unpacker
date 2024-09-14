import logging
from logging import getLogger, StreamHandler, Formatter
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DATA_PATH = Path("data")
BASE_PROCESS_PATH = BASE_DATA_PATH / "process"
STATUS_FILE = BASE_DATA_PATH / "status.json"
ENV_FILE_PATH = Path(".env")

COMPLETE_FLAG = "complete"
ITEMS_PROCESSED = "items_processed"
ITEM_COUNT = "items_count"
TOTAL_POSTS = "posts_count"
ACCEPTED_POSTS  = "accepted_posts"

## LOGGER
logger = getLogger("twitter-stream-unpacker")
handler = StreamHandler()
logger.addHandler(handler)
logger.propagate = False
handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
logger.setLevel(logging.INFO)


# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    LANGUAGES: list[str] = ["en", "es", "pt", "it", "de", "zxx", ]
    ONLY_ORIG_TWEETS: bool = True
    RESET_DB: bool = False
    DUMP_THRESH: int = 2000
    STORE_COMPLETE_CONTENT: bool = False
    # MIN DBS
    YEAR: int = 2022
    MONTH: int = 2


CONFIG = Config()
