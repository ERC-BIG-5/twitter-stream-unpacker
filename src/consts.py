import logging
from dataclasses import field
from logging import getLogger, StreamHandler, Formatter, FileHandler
from pathlib import Path
from typing import Literal, Optional

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_PATH = Path("/home/rsoleyma/projects/twitter-stream-unpacker")
BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_STAT_PATH = BASE_DATA_PATH / "stats"
LABELSTUDIO_TASK_PATH = BASE_DATA_PATH / "labelstudio_tasks"
ANNOTATED_BASE_PATH = BASE_DATA_PATH / "annotated"

ENV_FILE_PATH = PROJECT_PATH / ".env"

MAIN_DB = "MAIN"
ANNOTATION_DB = "ANNO"

## LOGGER
file_logs_path = BASE_DATA_PATH / "logs"
file_logs_path.mkdir(exist_ok=True)

logger = getLogger("twitter-stream-unpacker")


# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    LANGUAGES: list[str] = ["en", "es", "pt", "it", "de", "fr", "zxx"]
    # todo, remove that...
    ONLY_ORIG_TWEETS: bool = field(default=True, metadata={
        "description": "Filters out comments, retweets, quoted retweets..."})  # for main
    MAX_POSTS_PER_TIME_RANGE: int = 50
    RESET_DB: bool = False  # for main
    DUMP_THRESH: int = 2000  # for main, and create_min, DEPRECATED
    STORE_COMPLETE_CONTENT: bool = True  # for main, DEPRECATED
    TESTMODE = False  #
    # MIN DBS
    DB_LANGUAGE_SPLIT: bool = False
    YEAR: int = 2022,  # DEPRECATED
    MONTH: int = 2  # DEPRECATED
    # generic
    LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    FILE_LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # for something else,... setting up a pg db
    PG_PASSWORD: Optional[SecretStr] = None


CONFIG = Config()

if not logger.handlers:
    logger.propagate = False

    handler = StreamHandler()
    handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)

    file_handler = FileHandler(file_logs_path / "logs.txt")
    file_handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(file_handler)

    logger.setLevel(CONFIG.LOG_LEVEL)
    file_handler.setLevel(CONFIG.FILE_LOG_LEVEL)

ANNOT_EXTRA_TEST_ROUND = "1"
ANNOT_EXTRA_TEST_ROUND_EXPERIMENT = "1x"

# this is for the simple_generic_iter

locationindex_type = tuple[str, str, str, int]
