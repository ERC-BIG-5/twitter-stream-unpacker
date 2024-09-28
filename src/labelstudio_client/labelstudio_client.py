import datetime
import json
import os
from pathlib import Path

import label_studio_sdk as ls_sdk
from dotenv import load_dotenv
from label_studio_sdk.client import LabelStudio

BASE_DATA_PATH = Path("data")
BASE_LABEL_CONFIGS_PATH = BASE_DATA_PATH / "label_configs"
BASE_GENERATED_PROJECTS_PATH = BASE_DATA_PATH / "generated_projects"
GENERATED_PROJECTS_INFO_PATH = BASE_GENERATED_PROJECTS_PATH / "info.json"

for p in [BASE_DATA_PATH, BASE_LABEL_CONFIGS_PATH, BASE_GENERATED_PROJECTS_PATH]:
    if not p.exists():
        p.mkdir()

if not GENERATED_PROJECTS_INFO_PATH.exists():
    GENERATED_PROJECTS_INFO_PATH.write_text("[]", encoding="utf-8")


def create_api_client() -> LabelStudio:
    load_dotenv()
    from label_studio_sdk.client import LabelStudio

    # Connect to the Label Studio API and check the connection
    ls_base_uri = os.environ.get("LS_BASE_URL", "http://localhost:8080/")
    access_token = os.environ.get("LABELSTUDIO_ACCESS_TOKEN")
    ls_client = LabelStudio(base_url=ls_base_uri, api_key=access_token)
    return ls_client


def project_title(platform: str, year: int, month: int, language: str, annotation_extra: str):
    month_short_name = datetime.date(year=1, day=1, month=month).strftime("%b").lower()
    lang = language.ljust(3, "_")
    return f'{year}_{month_short_name}_{lang}_{platform}_{annotation_extra}'


def create_project(platform: str,
                   year: int,
                   month: int,
                   language: str,
                   annotation_extra: str,
                   label_config_name: str,
                   api_client: LabelStudio):
    title = project_title(platform, year, month, language, annotation_extra)
    label_config = (BASE_LABEL_CONFIGS_PATH / label_config_name).read_text(encoding="utf-8")
    project_data = api_client.projects.create(title=title, label_config=label_config, maximum_annotations=20)

    projects_info = json.load(GENERATED_PROJECTS_INFO_PATH.open(encoding="utf-8"))
    projects_info.append(project_data.dict())
    json.dump(projects_info, GENERATED_PROJECTS_INFO_PATH.open("w",encoding="utf-8"), ensure_ascii=False)
    return project_data


def delete_test_user() -> None:
    ls_client = create_api_client()
    for user in ls_client.users.list():
        print(user.username, user.id)
    # print(ls_client.users.list())
    # ls_client.users.delete(4)


def main() -> None:
    ls_client = create_api_client()
    # 1. create a new project
    # ls_client.projects.create()
    project_id = None
    # this one is important and allows multi-coder annotations
    for project in ls_client.projects.list():
        # print(project.title)
        project: ls_sdk.Project

        if project.title == "multicoder-test":
            print("p_id", project.id)
            project_id = project.id
            # print(project.maximum_annotations)

    # tested. works nice!:
    # print(ls_client.projects.update(5, maximum_annotations=2))

    # 2. create and sync local storage. remove it again afterward
    resp = ls_client.import_storage.local.create(
        title="local_import",
        path="/home/rsoleyma/projects/Annotation/local_storage",
        project=5
    )
    # print(resp)
    # print(resp.id)
    #
    storage_valid = ls_client.import_storage.local.validate(project=project_id, id=resp.id,
                                                            path="/home/rsoleyma/projects/Annotation/local_storage")
    # print(storage_valid)
    sync = ls_client.import_storage.local.sync(resp.id)
    print(sync)
    delete = ls_client.import_storage.local.delete(resp.id)
    print(delete)


if __name__ == "__main__":
    # main()
    # delete_test_user()
    p = create_project("twitter", 2022, 1, "en", "test",
                       "annotation_test.xml", create_api_client())
    print(p.dict())
