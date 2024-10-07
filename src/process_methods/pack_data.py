import gzip
from pathlib import Path
from typing import Any

from jsonlines import jsonlines
from pydantic import BaseModel, ConfigDict

from src.consts import locationindex_type, REPACK_BASE_PATH
from src.models import IterationSettings
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import year_month_str, json_gz_stem


class PackEntriesConfig(BaseModel):
    delete_jsonl_files: bool = True
    gzip_files: bool = True

    model_config = ConfigDict(extra='ignore')


class PackEntries(IterationMethod):
    """
    Filters posts that are in the selected languages and are original
    """

    def __init__(self, settings: IterationSettings, config: dict) -> None:
        super().__init__(settings, config)
        self.config = PackEntriesConfig.model_validate(config or {})

        self.base_path = REPACK_BASE_PATH / year_month_str(settings.year, settings.month)
        self.base_path.mkdir(exist_ok=True)
        # just initial parent path; will be replaced by REPACK_BASE_PATH/yyyy-mm/yyyy-mm-dd
        self.current_date_path = self.base_path
        self.current_day_loc_index: str = None
        self.current_jsonl_file: str = None
        self.fouts: dict[str, jsonlines.Writer] = {}
        # track if data is written, cuz we can delete empty files.
        self.fouts_data_written: dict[str, bool] = {}

    def zip_file(self, fp: Path):
        dest_fp = fp.parent / f"{fp.name}.gz"
        with open(fp, 'rt', encoding='utf-8') as f_in:
            with gzip.open(dest_fp, 'wb') as f_out:
                f_out.write(f_in.read().encode('utf-8'))

    def finalize_files(self):
        """
        close the writer, zip the file and delete the original
        Might just delete the file and not zip it, if there is no data
        Check config options, for deletion and zipping
        """
        for lang in self.settings.languages:
            existing_writer = self.fouts.get(lang)
            if existing_writer:
                existing_writer.close()
                jsonl_file = Path(existing_writer._fp.name)
                # check if we can just delete the file
                if not self.fouts_data_written[lang] and self.config.delete_jsonl_files:
                    jsonl_file.unlink()
                    continue
                if self.config.gzip_files:
                    self.zip_file(jsonl_file)
                if self.config.delete_jsonl_files:
                    jsonl_file.unlink()

    def init_new_lang_file_outs(self, filename: str):
        """
        Closes existing and reopens new jsonl-writers
        """
        self.finalize_files()
        for lang in self.settings.languages:
            lang_dir = self.current_date_path / lang
            lang_dir.mkdir(exist_ok=True)
            lang_fp = lang_dir / f"{filename}.jsonl"
            self.fouts[lang] = jsonlines.open(lang_fp, mode='w')
            self.fouts_data_written[lang] = False

    @property
    def name(self) -> str:
        return "pack"

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        if location_index[1] != self.current_day_loc_index:
            self.current_day_loc_index = location_index[1]
            self.current_date_path = self.base_path / location_index[1]
            self.current_date_path.mkdir(exist_ok=True)
            # todo close existing
        if location_index[2] != self.current_jsonl_file:
            self.init_new_lang_file_outs(json_gz_stem(location_index[2]))
        lang = post_data["lang"]
        self.fouts[lang].write(post_data)
        self.fouts_data_written[lang] = True

    def finalize(self):
        self.finalize_files()

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
