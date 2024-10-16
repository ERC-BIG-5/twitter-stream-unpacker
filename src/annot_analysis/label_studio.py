from dataclasses import dataclass, field
from typing import TypedDict, Any

NO_RESPONSE = "---"


class BaseExportData(TypedDict):
    id: str
    annotator: int
    annotation_id: int


CoderId = int


def default_factor(options: list[str]) -> dict[str, list[str]]:
    return {
        option: []
        for option in options
    }


AnnotationResult = TypedDict('AnnotationResult', {'id': int, "annotations": dict})


@dataclass
class ResultStruct:
    choices: dict[str, list[str]] = field(default_factory=dict)
    free_text: list[str] = field(default_factory=list)


def build_annotation_result_struct(struct: ResultStruct):
    result = {}
    for select_name, options in struct.choices.items():
        result[select_name] = {o: [] for o in options}
        result[select_name][NO_RESPONSE] = []

    for free_text_name in struct.free_text:
        result[free_text_name] = {}

    return result


def prepare_label_studio_export(export: list[BaseExportData], struct: ResultStruct) \
        -> tuple[dict[str, AnnotationResult], list[str]]:
    """
    returns results and missing ids
    """

    # task_id -> AnnotationResult
    annotations: dict[str, AnnotationResult] = {}
    missing = []
    for annotation in export:
        id = annotation["id"]
        if not annotation["annotation_id"]:
            missing.append(id)
            continue
        annotator = annotation["annotator"]
        annotation_result = annotations.setdefault(id, build_annotation_result_struct(struct))
        for annotation_key in struct.choices:
            res = annotation.get(annotation_key, NO_RESPONSE)

            annotation_result[annotation_key][res].append(annotator)
        for text_key in struct.free_text:
            text = annotation.get(text_key)
            if text:
                annotation_result[text_key][annotator] = text
    return annotations, missing
