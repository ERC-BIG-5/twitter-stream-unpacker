from logging import getLogger, StreamHandler, Formatter
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DATA_PATH = Path("data")
BASE_PROCESS_PATH = BASE_DATA_PATH / "process"
STATUS_FILE = BASE_DATA_PATH / "status.json"
ENV_FILE_PATH = Path(".env")

logger = getLogger("twitter-stream-unpacker")
handler = StreamHandler()
logger.addHandler(handler)
logger.propagate = False
handler.setFormatter(Formatter("%%(name)s - %(levelname)s - %(message)s"))

# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    STREAM_BASE_FOLDER:Path =  Path("/home/rsoleyma/big5-torrents")
    RESET_DB: bool = False
    SQLITE_FILE_PATH: Path



CONFIG = Config()
