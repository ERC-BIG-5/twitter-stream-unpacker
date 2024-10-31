import json
import shutil
from dataclasses import asdict, field
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, Field

from bert_sentence_classifier.experiment.sentence_embeddings.create_sentence_embeddings import get_post_text
from src.consts import logger, BASE_LABELSTUDIO_DATA_PATH
from src.util import get_hashtags


class LabelstudioTask(BaseModel):
    post_text: str
    post_url: Optional[str]
    has_media: bool = field(default=False)
    image: Optional[str] = None


class Nature4AxisTask(BaseModel):
    post_text: str

class TwitterImage(BaseModel):
    post_text: str
    image_0: Optional[str] = None
    image_1: Optional[str] = None
    image_2: Optional[str] = None
    image_3: Optional[str] = None

def dump_labelstudio_tasks(ls_tasks: list[Union[BaseModel,dict]], path: Path,
                           single_file: bool = False,
                           rewrite: bool = True):
    if path.exists():
        if rewrite:
            logger.info(f"Deleting existing labelstudio_tasks file/path: {path}")
            if path.is_dir():
                shutil.rmtree(str(path.absolute()))
            else:
                path.unlink()
        else:
            print(f"labelstudio tasks already exist: {path}, skipping, set rewrite to delete previous data")
            return

    def to_dict(task: Union[BaseModel,dict]):
        return task.model_dump() if not isinstance(task, dict) else task

    if single_file:
        with open(path, "w", encoding="utf-8") as fout:
            json.dump([to_dict(task) for task in ls_tasks], fout)
    else:
        path.mkdir(parents=True, exist_ok=True)
        for idx, task in enumerate(ls_tasks):
            (path / f"{str(idx)}.json").write_text(json.dumps(to_dict(task), ensure_ascii=False), encoding="utf-8")



# TaskInputTpe = "str" | "int" | "float" | "image"


class LabelStudioTaskInput(BaseModel):
    ls_type: str = Field(alias="type")  #
    id: int = Field(None)
    data: dict = Field(alias="data")
    metadata: dict = Field()


class LabelstudioTask(BaseModel):
    inputs: dict[str, LabelStudioTaskInput]


def create_labelstudio_tasks():
    pass


def create_nature4axis_tasks(entries: list[dict], task_file_name: str):
    tasks = []
    for e in entries:
        text = get_post_text(e)
        hashtags = get_hashtags(e)
        tasks.append(Nature4AxisTask.model_validate({"post_text": text}))

    dump_labelstudio_tasks(tasks, BASE_LABELSTUDIO_DATA_PATH / f"{task_file_name}.json", True)
