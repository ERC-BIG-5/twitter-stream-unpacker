from dataclasses import dataclass, field
from typing import TypedDict

NO_RESPONSE = "---"


class BaseExportData(TypedDict):
    id: int
    annotator: int


CoderId: int


def default_factor(options: list[str]) -> dict[str, list[str]]:
    return {
        option: []
        for option in options
    }


@dataclass
class Nature4AxisResult:
    relevant: TypedDict("relevant",
                        {"Relevant": list[CoderId], "Not-relevant": list[CoderId], "Uncertain": list[CoderId]}) = field(
        default_factory=lambda _: {
            "Relevant": [], "Not-relevant": [], "Uncertain": []
        })

    non_human: dict[str, list[CoderId]]
    material: dict[str, list[CoderId]]
    life: dict[str, list[CoderId]]
    ideal_state: dict[str, list[CoderId]]


class Nature4Axis(TypedDict):
    relevant: TypedDict("relevant",
                        {"Relevant": list[CoderId], "Not-relevant": list[CoderId], "Uncertain": list[CoderId]})
    non_human: dict[str, list[CoderId]]
    material: dict[str, list[CoderId]]
    life: dict[str, list[CoderId]]
    ideal_state: dict[str, list[CoderId]]


AnnotationResult = TypedDict('AnnotationResult', {'id': int, "annotations":
    TypedDict("Annotation", Nature4Axis)})


def prepare_label_studio_export(export: list[BaseExportData], annotation_keys: list[str], input_keys: list[str] = None):
    annotations: dict[int, AnnotationResult] = {}
    for annotation in export:
        annotation_result = annotations.setdefault(annotation["id"], {"id": annotation["id"], "annotations": {}})
        for annotation_key in annotation_keys:
            res = annotation.get(annotation_key, NO_RESPONSE)
            annotation_result[annotation_key]
    return AnnotationResult
