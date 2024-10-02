import datetime
import json
from dataclasses import dataclass

import label_studio_sdk as ls_sdk
from label_studio_sdk.client import LabelStudio
from label_studio_sdk.core import ApiError

from src.consts import LABELSTUDIO_LABEL_CONFIGS_PATH, GENERATED_PROJECTS_INFO_PATH, CONFIG
from src.models import IterationSettings


@dataclass
class ProjectInfo:
    title: str
    id: int


class LabelStudioManager:

    def __init__(self):
        self.ls_client = LabelStudio(base_url=CONFIG.LS_BASE_URL,
                                     api_key=CONFIG.LABELSTUDIO_ACCESS_TOKEN)

        self.get_projects_list()

    def get_projects_list(self) -> list[ProjectInfo]:
        """
        we can use this to get an overview, in case we lost it...
        """
        page = self.ls_client.projects.list()
        projects_list: list[ProjectInfo] = []
        while page is not None:
            for project in page.items:
                projects_list.append(ProjectInfo(project.title, project.id))
            try:
                page = page.next_page()
            except ApiError as e:
                page = None
        return projects_list

    def _project_title(self, platform: str, year: int, month: int, language: str, annotation_extra: str):
        month_short_name = datetime.date(year=1, day=1, month=month).strftime("%b").lower()
        return f'{year}_{month_short_name}_{language}_{platform}_{annotation_extra}'

    def create_project(self, platform: str,
                       settings: IterationSettings,
                       label_config_name: str,
                       language: str) -> int:
        """
        return the id
        """
        # we ignore the language in the settings, but take the one passed as param
        title = self._project_title(platform, settings.year, settings.month, language,
                                    settings.annotation_extra)
        label_config = (LABELSTUDIO_LABEL_CONFIGS_PATH / label_config_name).read_text(encoding="utf-8")

        project_data = self.ls_client.projects.create(title=title,
                                                      label_config=label_config,
                                                      maximum_annotations=20)

        # NICE TO HAVE
        projects_info = json.load(GENERATED_PROJECTS_INFO_PATH.open(encoding="utf-8"))
        projects_info.append(project_data.dict())
        json.dump(projects_info, GENERATED_PROJECTS_INFO_PATH.open("w", encoding="utf-8"), ensure_ascii=False)
        return project_data.id

    def create_projects_for_db(self, platform, settings: IterationSettings, label_config_name) -> dict[str, int]:
        return {language: self.create_project(platform, settings, label_config_name, language)
                for language in settings.languages}

    def _delete_all_projects(self):
        for project in self.get_projects_list():
            self.ls_client.projects.delete(project.id)

# def delete_test_user() -> None:
#     ls_client = create_api_client()
#     for user in ls_client.users.list():
#         print(user.username, user.id)
#     # print(ls_client.users.list())
#     # ls_client.users.delete(4)


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
    ls_mgmt = LabelStudioManager()
    ls_mgmt._delete_all_projects()
    # p = ls_mgmt.create_project("twitter", IterationSettings(2022, 1, {"en"}, CONFIG.ANNOT_EXTRA,
    #                                                         ), CONFIG.LABELSTUDIO_LABEL_CONFIG_FILENAME)
    # print(p.dict())
