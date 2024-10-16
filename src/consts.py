import logging
from datetime import datetime
from logging import getLogger, StreamHandler, Formatter, FileHandler
from pathlib import Path
from typing import Literal, Optional

from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_PATH = Path("/home/rsoleyma/projects/twitter-stream-unpacker")
BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_METHODS_CONFIG_PATH = BASE_DATA_PATH / "method_configs"

BASE_DBS_PATH = BASE_DATA_PATH / "sqlite_dbs"
BASE_STAT_PATH = BASE_DATA_PATH / "stats"
BASE_REPACK_PATH = BASE_DATA_PATH / "repack"
MAIN_STATUS_FILE_PATH = BASE_DATA_PATH / "status.json"
AUTO_RELEVANT_COLLECTION = BASE_DATA_PATH / "auto-relevant"

ANNOTATED_BASE_PATH = BASE_DATA_PATH / "annotated"
LOGS_BASE_PATH = BASE_DATA_PATH / "logs"

BASE_LABELSTUDIO_DATA_PATH = BASE_DATA_PATH / "labelstudio"

LABELSTUDIO_LABEL_CONFIGS_PATH = BASE_LABELSTUDIO_DATA_PATH / "label_configs"
# BASE_GENERATED_PROJECTS_PATH = BASE_DATA_PATH / "generated_projects"
GENERATED_PROJECTS_INFO_PATH = BASE_LABELSTUDIO_DATA_PATH / "info.json"

ANNOT_EXTRA_TEST_ROUND = "1"
ANNOT_EXTRA_TEST_ROUND_EXPERIMENT = "1x"
ANOOT_EXTRA_TEST_HAS_MEDIA = "1m"

for p in [BASE_DATA_PATH, BASE_DBS_PATH, BASE_METHODS_CONFIG_PATH, BASE_REPACK_PATH, BASE_STAT_PATH, ANNOTATED_BASE_PATH, LOGS_BASE_PATH,
          BASE_LABELSTUDIO_DATA_PATH, LABELSTUDIO_LABEL_CONFIGS_PATH, AUTO_RELEVANT_COLLECTION]:
    p.mkdir(parents=True, exist_ok=True)

if not GENERATED_PROJECTS_INFO_PATH.exists():
    GENERATED_PROJECTS_INFO_PATH.write_text("[]", encoding="utf-8")

ENV_FILE_PATH = PROJECT_PATH / ".env"

MAIN_DB = "MAIN"
ANNOTATION_DB = "ANNO"

## LOGGER
logger = getLogger("twitter-stream-unpacker")

DATA_SOURCE_DUMP = "dump"
DATA_SOURCE_REPACK = "repack"


# CONFIG
class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    DATA_SOURCE: Literal["dump"] | Literal["repack"]
    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    LANGUAGES: list[str] = ["en"]  # ["en", "es", "pt", "it", "de", "fr", "zxx"]
    RESET_DATA: bool = False  # for main
    ANNOT_EXTRA: str = ANNOT_EXTRA_TEST_ROUND
    TEST_MODE: bool = False  #
    YEAR: int = 2022
    MONTH: int = 1
    METHODS: list[str] = []
    METHODS_CONFIG_FILE: Optional[str]
    CONFIRM_RUN: bool = True
    LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    FILE_LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "WARNING"
    # for something else,... setting up a pg db
    PG_PASSWORD: Optional[SecretStr] = None
    # LABLESTUDIO
    LS_BASE_URL: Optional[str] = "http://localhost:8080/"
    LABELSTUDIO_ACCESS_TOKEN: Optional[str] = None
    LABELSTUDIO_TASK_PATH: str
    LABELSTUDIO_CONFIG_TASK_BASE_PATH: str  # this is to specify the dataset path for LS (specially when it's running in docker)
    KEEP_LABELSTUDIO_TASKS: bool = Field(False, description="After creating the task files, they can be deleted")
    # TODO this for later will be related to annotation_extra (maybe name it experiment)
    LABELSTUDIO_LABEL_CONFIG_FILENAME: str = "annotation_test.xml"
    # TEST MODE
    TEST_NUM_TAR_FILES: int = Field(1, ge=1)
    TEST_NUM_JSONL_FILES: int = Field(20)


CONFIG = Config()  # type: ignore[call-arg]

if not logger.handlers:
    logger.propagate = False

    handler = StreamHandler()
    handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)

    start_time_str = datetime.now().strftime("%Y%m%d-%H%M")

    file_handler = FileHandler(LOGS_BASE_PATH / "logs.txt")
    file_handler.setFormatter(Formatter(f"({start_time_str})-%(levelname)s: %(message)s"))
    logger.addHandler(file_handler)

    logger.setLevel(CONFIG.LOG_LEVEL)
    file_handler.setLevel(CONFIG.FILE_LOG_LEVEL)


def get_logger(fn: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("twitter-stream-unpacker." + fn)
    # handler = StreamHandler()
    # handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    # logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# this is for the simple_generic_iter

locationindex_type = tuple[str, str, str, int]

# METHODS
METHOD_FILTER = "filter"
METHOD_STATS = "stats"
METHOD_INDEX_DB = "index"
METHOD_SCHEMA = "schema"
METHOD_ANNOTATION_DB = "annotation"
METHOD_MEDIA_FILTER = "media-filter"
METHOD_REPACK = "repack"
METHOD_AUTO_RELEVANCE = "auto_relevance"
