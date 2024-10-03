import datetime
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from label_studio_sdk.client import LabelStudio
from label_studio_sdk.core import ApiError

from src.consts import LABELSTUDIO_LABEL_CONFIGS_PATH, GENERATED_PROJECTS_INFO_PATH, CONFIG
from src.db.db import main_db_path2
from src.labelstudio.create_tasks.test_annotation import create_annotation_label_ds
from src.models import IterationSettings, SingleLanguageSettings


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

    def create_project(self,
                       platform: str,
                       settings: SingleLanguageSettings,
                       label_config_name: str) -> int:
        """
        return the project.id
        """
        # we ignore the language in the settings, but take the one passed as param
        title = self._project_title(platform, settings.year, settings.month, settings.language,
                                    settings.annotation_extra)
        label_config = (LABELSTUDIO_LABEL_CONFIGS_PATH / label_config_name).read_text(encoding="utf-8")

        project_data = self.ls_client.projects.create(title=title,
                                                      label_config=label_config,
                                                      maximum_annotations=20)

        # NICE TO HAVE
        projects_info = json.load(GENERATED_PROJECTS_INFO_PATH.open(encoding="utf-8"))
        projects_info.append(project_data.dict())
        json.dump(projects_info, GENERATED_PROJECTS_INFO_PATH.open("w", encoding="utf-8"), ensure_ascii=False)

        # this accesses the database and creates json-files
        labelstudio_task_path = self.get_labelstudio_task_path(settings)
        create_annotation_label_ds(settings, labelstudio_task_path)
        # use local_storage feature of labelstudio to import tasks
        self.import_ds_to_labelstudio(labelstudio_task_path, project_data.id)
        if not CONFIG.KEEP_LABELSTUDIO_TASKS:
            self.delete_labelstudio_tasks(labelstudio_task_path)
        return project_data.id

    def create_projects_for_db(self, platform, settings: IterationSettings, label_config_name) -> dict[str, int]:
        """
        create project for all each language and import its annotations
        @returns: dict[language:project.id]
        """
        results: dict[str, int] = {}
        for language in settings.languages:
            lang_settings = SingleLanguageSettings.from_iter_settings(settings, language)
            results[language] = self.create_project(platform, lang_settings, label_config_name)
        return results

    def delete_labelstudio_tasks(self, labelstudio_tasks_path: Path):
        if labelstudio_tasks_path.is_dir():
            shutil.rmtree(labelstudio_tasks_path)
        else:
            labelstudio_tasks_path.unlink(missing_ok=True)


    def _delete_all_projects(self):
        for project in self.get_projects_list():
            self.ls_client.projects.delete(project.id)

    def import_ds_to_labelstudio(self, ds_task_path: Path, project_id: int):
        ds_rel_path = ds_task_path.relative_to(CONFIG.LABELSTUDIO_TASK_PATH)
        ls_stuio_relative_path = CONFIG.LABELSTUDIO_CONFIG_TASK_BASE_PATH / ds_rel_path
        try:
            resp = self.ls_client.import_storage.local.create(
                title="local_import",
                path=ls_stuio_relative_path.as_posix(),
                project=project_id
            )
            self.ls_client.import_storage.local.validate(project=project_id, id=resp.id,
                                                                         path=ls_stuio_relative_path.as_posix())
            sync = self.ls_client.import_storage.local.sync(resp.id)
            print(sync)
            self.ls_client.import_storage.local.delete(resp.id)
        except ApiError as e:
            print(e)
            return

    @staticmethod
    def get_labelstudio_task_path(settings: SingleLanguageSettings,
                                  single_file: bool = False) -> Path:
        f_stem = main_db_path2(settings).stem
        if single_file:
            return Path(CONFIG.LABELSTUDIO_TASK_PATH) / f"{f_stem}.json"
        else:
            return Path(CONFIG.LABELSTUDIO_TASK_PATH) / f_stem


# def delete_test_user() -> None:
#     ls_client = create_api_client()
#     for user in ls_client.users.list():
#         print(user.username, user.id)
#     # print(ls_client.users.list())
#     # ls_client.users.delete(4)


if __name__ == "__main__":
    # main()
    # delete_test_user()
    ls_mgmt = LabelStudioManager()
    ls_mgmt._delete_all_projects()
    # p = ls_mgmt.create_project("twitter", IterationSettings(2022, 1, {"en"}, CONFIG.ANNOT_EXTRA,
    #                                                         ), CONFIG.LABELSTUDIO_LABEL_CONFIG_FILENAME)
    # print(p.dict())
