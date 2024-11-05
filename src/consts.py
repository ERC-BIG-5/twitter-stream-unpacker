import json
import logging
import sys
from datetime import datetime
from logging import getLogger, StreamHandler, Formatter, FileHandler
from pathlib import Path
from typing import Literal, Optional, Any

from pydantic import SecretStr, Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_PATH = Path(".")
BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_RUN_CONFIGS_PATH = BASE_DATA_PATH / "_0_run_configs"

BASE_DBS_PATH = BASE_DATA_PATH / "sqlite_dbs"
BASE_STAT_PATH = BASE_DATA_PATH / "stats"
BASE_REPACK_PATH = BASE_DATA_PATH / "repack"
MAIN_STATUS_FILE_PATH = BASE_DATA_PATH / "status.json"
AUTO_RELEVANT_COLLECTION = BASE_DATA_PATH / "auto-relevant"
BASE_MEDIA_FOLDER = BASE_DATA_PATH / "media"
BASE_TEST_PATH = BASE_DATA_PATH / "test"
BAGS_BASE_PATH = BASE_DATA_PATH / "bags"

ANNOTATED_BASE_PATH = BASE_DATA_PATH / "annotated"
LOGS_BASE_PATH = BASE_DATA_PATH / "logs"

BASE_LABELSTUDIO_DATA_PATH = BASE_DATA_PATH / "labelstudio"

LABELSTUDIO_LABEL_CONFIGS_PATH = BASE_LABELSTUDIO_DATA_PATH / "label_configs"
# BASE_GENERATED_PROJECTS_PATH = BASE_DATA_PATH / "generated_projects"
GENERATED_PROJECTS_INFO_PATH = BASE_LABELSTUDIO_DATA_PATH / "info.json"

ANNOT_EXTRA_TEST_ROUND = "1"
ANNOT_EXTRA_TEST_ROUND_EXPERIMENT = "1x"
ANOOT_EXTRA_TEST_HAS_MEDIA = "1m"

ENV_FILE_PATH = PROJECT_PATH / ".env"

if not ENV_FILE_PATH.exists():
    print(".env file exists, which means you are probably starting the script from the wrong directory...")
    print(Path().absolute().as_posix())
    print("BYE")
    sys.exit(1)

for p in [BASE_DATA_PATH, BASE_DBS_PATH, BASE_RUN_CONFIGS_PATH, BASE_TEST_PATH, BASE_REPACK_PATH, BASE_STAT_PATH, ANNOTATED_BASE_PATH, LOGS_BASE_PATH,
          BASE_LABELSTUDIO_DATA_PATH, LABELSTUDIO_LABEL_CONFIGS_PATH, AUTO_RELEVANT_COLLECTION, BAGS_BASE_PATH]:
    p.mkdir(parents=True, exist_ok=True)

if not GENERATED_PROJECTS_INFO_PATH.exists():
    GENERATED_PROJECTS_INFO_PATH.write_text("[]", encoding="utf-8")



MAIN_DB = "MAIN"
ANNOTATION_DB = "ANNO"

## LOGGER
logger = getLogger("twitter-stream-unpacker")

DATA_SOURCE_DUMP = "dump"
DATA_SOURCE_REPACK = "repack"
DATA_SOURCE_RANDOM_REPACK = "random_repack"

class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    CONF_JSON: str
    STREAM_BASE_FOLDER: Path = Path("/home/rsoleyma/big5-torrents")
    RESET_DATA: bool = False  # for main
    TEST_MODE: bool = False  #
    CONFIRM_RUN: bool = True
    LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    FILE_LOG_LEVEL: Literal["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"] = "WARNING"
    # for something else,... setting up a pg db
    PG_USER_NAME: Optional[str] = None
    PG_PASSWORD: Optional[SecretStr] = None
    PG_HOSTNAME: Optional[str] = None
    PG_PORT: Optional[int] = None
    PG_DB_NAME: Optional[str] = None

    # LABLESTUDIO
    LS_BASE_URL: Optional[str] = "http://localhost:8080/"
    LABELSTUDIO_ACCESS_TOKEN: Optional[str] = None
    LABELSTUDIO_TASK_PATH: str
    LABELSTUDIO_CONFIG_TASK_BASE_PATH: str  # this is to specify the dataset path for LS (specially when it's running in docker)
    KEEP_LABELSTUDIO_TASKS: bool = Field(False, description="After creating the task files, they can be deleted")
    # TODO this for later will be related to annotation_extra (maybe name it experiment)
    LABELSTUDIO_LABEL_CONFIG_FILENAME: str = "annotation_test.xml"
    # TEST MODE
    TEST_NUM_TAR_FILES: int = Field(1, ge=1, description="number of days (in repack)")
    TEST_NUM_JSONL_FILES: int = Field(20)


# CONFIG
class Config(BaseModel):
    DATA_SOURCE: Literal["dump"] | Literal["repack"] | Literal["random_repack"]
    LANGUAGES: Optional[list[str]] = None  # ["en", "es", "pt", "it", "de", "fr", "zxx"]
    RESET_DATA: bool = False  # for main
    ANNOT_EXTRA: str = ANNOT_EXTRA_TEST_ROUND
    YEAR: int = 2022
    MONTH: int
    DAYS: Optional[list[int]] = None
    COLLECTION_LIMIT: int = -1 # used random_repack
    METHODS: list[str] = []
    METHODS_CONFIG: dict[str,dict[str,Any]] = Field(default_factory=dict)

ENV_SETTINGS = EnvSettings()


conf_file = BASE_RUN_CONFIGS_PATH / ENV_SETTINGS.CONF_JSON
conf = json.load(conf_file.open(encoding="utf-8"))
CONFIG = Config.model_validate(conf)


file_logger = getLogger("twitter-stream-unpacker-fl")
start_time_str = datetime.now().strftime("%Y%m%d-%H%M")
_file_handler = FileHandler(LOGS_BASE_PATH / f"fl-{start_time_str}.txt")
_file_handler.setFormatter(Formatter(f"%(message)s"))
file_logger.addHandler(_file_handler)
file_logger.setLevel("INFO")

if not logger.handlers:
    logger.propagate = False
    handler = StreamHandler()
    # handler.setFormatter(Formatter("%(levelname)s: %(message)s"))
    handler.setFormatter(Formatter("%(levelname)s: %(funcName)s %(message)s"))
    logger.addHandler(handler)

    file_handler = FileHandler(LOGS_BASE_PATH / "logs.txt")
    file_handler.setFormatter(Formatter(f"({start_time_str})-%(levelname)s:  %(message)s"))
    logger.addHandler(file_handler)

    logger.setLevel(ENV_SETTINGS.LOG_LEVEL)
    file_handler.setLevel(ENV_SETTINGS.FILE_LOG_LEVEL)


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
METHOD_ANNOTATION_DB = "annotation" # depr.
METHOD_REPACK = "repack"
METHOD_AUTO_RELEVANCE = "auto_relevance"
METHOD_FIND = "find" # temp