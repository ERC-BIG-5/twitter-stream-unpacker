import logging
from dataclasses import field
from logging import getLogger, StreamHandler, Formatter, FileHandler
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_PATH = Path("/home/rsoleyma/projects/twitter-stream-unpacker")
BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_STAT_PATH = BASE_DATA_PATH / "stats"
ENV_FILE_PATH = Path(".env")

MAIN_DB = "MAIN"
ANNOTATION_DB = "ANNO"



## LOGGER
file_logs_path = BASE_DATA_PATH / "logs"
file_logs_path.mkdir(exist_ok=True)
logger = getLogger("twitter-stream-unpacker")
logger.propagate = False

handler = StreamHandler()
handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
logger.addHandler(handler)

file_handler = FileHandler(file_logs_path / "logs.txt")
file_handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
logger.addHandler(file_handler)




# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    LANGUAGES: list[str] = ["en", "es", "pt", "it", "de"]
    ONLY_ORIG_TWEETS: bool = field(default=True, metadata={"description": "Filtes out comments, retweets, quoted retweets..."}) # for main
    MAX_POSTS_PER_TIME_RANGE:int = 50
    RESET_DB: bool = False # for main
    DUMP_THRESH: int = 2000 # for main, and create_min
    STORE_COMPLETE_CONTENT: bool = True # for main
    DB_LANGUAGE_SPLIT: bool = False
    # MIN DBS
    YEAR: int = 2022
    MONTH: int = 2
    # generic
    LOG_LEVEL: Literal["INFO","DEBUG","WARNING", "ERROR", "CRITICAL"] = "INFO"
    FILE_LOG_LEVEL: Literal["INFO","DEBUG","WARNING", "ERROR", "CRITICAL"] = "INFO"


CONFIG = Config()

logger.setLevel(CONFIG.LOG_LEVEL)
file_handler.setLevel(CONFIG.FILE_LOG_LEVEL)
