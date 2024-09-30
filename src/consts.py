import logging
from dataclasses import field
from logging import getLogger, StreamHandler, Formatter, FileHandler
from pathlib import Path
from typing import Literal, Optional

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_PATH = Path("/home/rsoleyma/projects/twitter-stream-unpacker")
BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_DBS_PATH = BASE_DATA_PATH / "sqlite_dbs"
BASE_STAT_PATH = BASE_DATA_PATH / "stats"
MAIN_STATUS_FILE_PATH = BASE_DATA_PATH / "status.json"

ANNOTATED_BASE_PATH = BASE_DATA_PATH / "annotated"
LOGS_BASE_PATH = BASE_DATA_PATH / "logs"

BASE_LABELSTUDIO_DATA_PATH = BASE_DATA_PATH / "labelstudio"
LABELSTUDIO_TASK_PATH = BASE_LABELSTUDIO_DATA_PATH / "labelstudio_tasks"
LABELSTUDIO_LABEL_CONFIGS_PATH = BASE_LABELSTUDIO_DATA_PATH / "label_configs"
#BASE_GENERATED_PROJECTS_PATH = BASE_DATA_PATH / "generated_projects"
GENERATED_PROJECTS_INFO_PATH = BASE_LABELSTUDIO_DATA_PATH / "info.json"



for p in [BASE_DATA_PATH, BASE_DBS_PATH, BASE_STAT_PATH, LABELSTUDIO_TASK_PATH, ANNOTATED_BASE_PATH,LOGS_BASE_PATH,
          BASE_LABELSTUDIO_DATA_PATH, LABELSTUDIO_TASK_PATH,LABELSTUDIO_LABEL_CONFIGS_PATH]:
    p.mkdir(parents=True, exist_ok=True)

if not GENERATED_PROJECTS_INFO_PATH.exists():
    GENERATED_PROJECTS_INFO_PATH.write_text("[]", encoding="utf-8")


ENV_FILE_PATH = PROJECT_PATH / ".env"

MAIN_DB = "MAIN"
ANNOTATION_DB = "ANNO"

## LOGGER
logger = getLogger("twitter-stream-unpacker")


# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    LANGUAGES: list[str] = ["en"] #["en", "es", "pt", "it", "de", "fr", "zxx"]
    # todo, remove that...
    ONLY_ORIG_TWEETS: bool = field(default=True, metadata={
        "description": "Filters out comments, retweets, quoted retweets..."})  # for main
    MAX_POSTS_PER_TIME_RANGE: int = 50
    RESET_DB: bool = False  # for main
    DUMP_THRESH: int = 2000  # for main, and create_min, DEPRECATED
    STORE_COMPLETE_CONTENT: bool = True  # for main, DEPRECATED
    TESTMODE:bool = False  #
    # MIN DBS
    DB_LANGUAGE_SPLIT: bool = False
    YEAR: int = 2022  # DEPRECATED
    MONTH: int = 2  # DEPRECATED
    # generic
    LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    FILE_LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # for something else,... setting up a pg db
    PG_PASSWORD: Optional[SecretStr] = None
    # LABLESTUDIO
    LS_BASE_URL: Optional[str] = "http://localhost:8080/"
    LABELSTUDIO_ACCESS_TOKEN: Optional[str] = None


CONFIG = Config()

if not logger.handlers:
    logger.propagate = False

    handler = StreamHandler()
    handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)

    file_handler = FileHandler(LOGS_BASE_PATH / "logs.txt")
    file_handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(file_handler)

    logger.setLevel(CONFIG.LOG_LEVEL)
    file_handler.setLevel(CONFIG.FILE_LOG_LEVEL)

ANNOT_EXTRA_TEST_ROUND = "1"
ANNOT_EXTRA_TEST_ROUND_EXPERIMENT = "1x"

# this is for the simple_generic_iter

locationindex_type = tuple[str, str, str, int]

