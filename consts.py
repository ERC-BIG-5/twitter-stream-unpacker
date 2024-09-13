import logging
from logging import getLogger, StreamHandler, Formatter
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DATA_PATH = Path("data")
BASE_PROCESS_PATH = BASE_DATA_PATH / "process"
STATUS_FILE = BASE_DATA_PATH / "status.json"
ENV_FILE_PATH = Path(".env")

COMPLETE_FLAG = "_COMPLETE"

logger = getLogger("twitter-stream-unpacker")
handler = StreamHandler()
logger.addHandler(handler)

logger.propagate = False
handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
logger.setLevel(logging.DEBUG)

# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    LANGUAGES: list[str] = ["en", "es", "zxx", "pt"]
    ONLY_ORIG_TWEETS: bool = True
    DUMP_THRESH:int = 2000
    STORE_COMPLETE_CONTENT:bool = False
    # MIN DBS
    YEAR:int = 2022
    MONTH: int = 2

CONFIG = Config()
