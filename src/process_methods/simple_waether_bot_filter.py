import json
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
from gensim.models import KeyedVectors
from pydantic import BaseModel

from bert_sentence_classifier.experiment.sentence_embeddings.create_sentence_embeddings import get_sentence_embedding
from bert_sentence_classifier.model import BinaryClassification
from src.consts import locationindex_type
from src.models import IterationSettings, ProcessCancel
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus
from src.util import get_post_text


class WeatherBotFilter(BaseModel):
    bot_vectors_file: Path
    human_vectors_file: Path


class SimpleWeatherBotFilter(IterationMethod):

    def __init__(self, settings: IterationSettings, config: WeatherBotFilter):
        super().__init__(settings, config)

        self.kv: KeyedVectors = None

        for type, file_p in [("b",config.bot_vectors_file), ("h", config.human_vectors_file)]:
            with open(config.bot_vectors_file, encoding="utf-8") as f:
                vectors = json.load(f)
                if not self.kv:
                    self.kv = KeyedVectors(len(vectors[0]))
                self.kv.add_vectors([f"{type}_{i}" for i in list(range(len(vectors)))],
                               [np.array(v) for v in vectors])

        model = BinaryClassification.load_split_model("split_model")

        self.get_sentence_embedding = partial(get_sentence_embedding, model.bert_model, model.get_tokenizer())
        self.c = 0

    @staticmethod
    def name() -> str:
        return "bot-filter1"

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        text = get_post_text(post_data)
        embedding = self.get_sentence_embedding(text)
        self.c += 1
        closest_embeddings = self.kv.similar_by_vector(embedding, topn=3)
        for e in closest_embeddings:
            if e[0][0] == "h":
                print("HH", text, post_data["text"], post_data["id"])
                return
        print(text)
        return ProcessCancel("likely bot generated")


    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass
