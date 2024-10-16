from csv import DictWriter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypedDict, Any

NO_RESPONSE = "000"


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


@dataclass
class TaskResultsAnnotations:
    choices: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    free_texts: dict = field(default_factory=dict)  # [str, list[str]]


@dataclass
class TaskResults:
    inputs: dict[str, str] = field(default_factory=dict)
    annotations: TaskResultsAnnotations = field(default_factory=TaskResultsAnnotations)


AnnotationResult = TypedDict('AnnotationResult', {"annotations": dict, "inputs": dict})


@dataclass
class ResultStruct:
    choices: dict[str, list[str]] = field(default_factory=dict)
    free_text: list[str] = field(default_factory=list)
    inputs: list[str] = field(default_factory=list)


def build_annotation_result_struct(struct: ResultStruct) -> TaskResults:
    result: TaskResults = TaskResults()
    for select_name, options in struct.choices.items():
        result.annotations.choices[select_name] = {o: [] for o in options}
        result.annotations.choices[select_name][NO_RESPONSE] = []

    for free_text_name in struct.free_text:
        result.annotations.free_texts[free_text_name] = {}

    return result


# def get_annotation_inputs(export: list[BaseExportData], input_keys: list[str]) -> dict[str, dict[str, Any]]:
#     result: dict[str, dict[str, Any]] = {}
#     for entry in export:
#         if entry["id"] not in result:
#             result[entry["id"]] = {
#                 input: entry[input] for input in input_keys
#             }
#     return result


def prepare_label_studio_export(export: list[BaseExportData], struct: ResultStruct) \
        -> tuple[dict[str, TaskResults], list[str]]:
    """
    returns results and missing ids
    """

    # task_id -> AnnotationResult
    annotations: dict[str, TaskResults] = {}
    missing = []
    for annotation in export:
        id = annotation["id"]
        if not annotation["annotation_id"]:
            missing.append(id)
            continue
        annotator = annotation["annotator"]
        annotation_result: TaskResults
        if id not in annotations:
            annotation_result = build_annotation_result_struct(struct)
            annotations[id] = annotation_result
            annotation_result.inputs = {input: annotation.get(input) for input in struct.inputs}

        for annotation_key in struct.choices:
            res = annotation.get(annotation_key, NO_RESPONSE)

            annotation_result.annotations.choices[annotation_key][res].append(annotator)
        for text_key in struct.free_text:
            text = annotation.get(text_key)
            if text:
                annotation_result.annotations.free_texts[text_key][annotator] = text
    return annotations, missing

def get_user_name(user_id: int) -> str:
    # todo get that data
    user_lookup = {}
    return user_lookup.get(user_id, str(user_id))

def results2csv(result_struct: ResultStruct, results: dict[str, TaskResults], destination: Path):
    # build_rows
    field_names: list[str] = ["task"]
    notes_columns: list[str] = []  # will be added, when needed

    # append fieldnames for task input
    for input in result_struct.inputs:
        field_names.append(input)

    def choice_col_name(choice:str, option:str) -> str:
        return f"{choice}-{option}"

    # append fieldnames for choices
    for choice, options in result_struct.choices.items():
        for option in options:
            field_names.append(choice_col_name(choice, option))
        field_names.append(choice_col_name(choice, NO_RESPONSE))

    # build rows
    rows: list[dict[str, Any]] = []
    for task_id, task_results in results.items():
        row = {
            "task": task_id
        }
        for input in result_struct.inputs:
            row[input] = task_results.inputs[input]
        for choice, options in task_results.annotations.choices.items():
            for option in options:
                option_results = options[option]
                user_names = [get_user_name(o) for o in option_results]
                row[choice_col_name(choice, option)] = "; ".join(user_names)
        rows.append(row)

    # print(field_names)
    writer = DictWriter(destination.open("w", encoding="utf-8"), fieldnames=field_names)
    writer.writeheader()
    writer.writerows(rows)
    return rows