import json

from src.annot_analysis.label_studio import prepare_label_studio_export, results2csv
from src.consts import BASE_LABELSTUDIO_DATA_PATH, BASE_DATA_PATH, ANNOTATED_BASE_PATH
from src.labelstudio.labelstudio_client import LabelStudioManager
from src.labelstudio.parse_config import parse_label_config_xml

PROJECT_IDS = [2]

# todo, store in file, fetch it from API
PROJECT_CONFIGS = {
    2: "annotation_nature1.xml"
}


def main():
    label_configs_base = BASE_LABELSTUDIO_DATA_PATH / "label_configs"
    for project in PROJECT_IDS:
        config_fp = label_configs_base / PROJECT_CONFIGS[project]
        config_types = parse_label_config_xml(config_fp.read_text())

        # ls_mgmt = LabelStudioManager()
        # annotations = ls_mgmt.get_project_annotations(project)
        # json.dump(annotations, (BASE_DATA_PATH / "temp/annotations_JSON_MIN.json").open("w",encoding="utf-8"))
        # TODO FOR TESTING
        annotations = json.load((BASE_DATA_PATH / "temp/annotations_JSON_MIN.json").open(encoding="utf-8"))
        #
        results, missing = prepare_label_studio_export(annotations, config_types)
        # inputs = get_annotation_inputs(annotations)
        # print(json.dumps(results, indent=2))
        dest = ANNOTATED_BASE_PATH / "Nature-4Axis/res.csv"
        print(results2csv(config_types, results,dest))

if __name__ == "__main__":
    main()
