from collections import Counter
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from random import choices
from typing import Any

from jsonlines import jsonlines
from tqdm import tqdm

from src.util import iter_jsonl_file, list_jsonl_file

other_keys: set = set()

def data_ana_ref(tweet: dict)-> str:
    return tweet["data"].get("referenced_tweets", [{}])[0].get("type", "original")

def data_ana_lang(tweet: dict)-> str:
    return tweet["data"]["lang"]


def count_all_langs(source_path: Path) -> Counter:
    lang_counter = Counter()
    for jsonl_fp in tqdm(list(list_jsonl_file(source_path))):
        for jsonl_data in iter_jsonl_file(jsonl_fp):
            lang_counter[jsonl_data["data"]["lang"]] += 1
    return lang_counter


def count_orig_posts(source_path: Path) -> int:
    count = 0
    for jsonl_fp in tqdm(list(list_jsonl_file(source_path))):
        for jsonl_data in iter_jsonl_file(jsonl_fp):
            if jsonl_data.get("referenced_tweets") is None:
                count +=1
    return count

def ana_structure(source_path: Path) -> dict:
    def count_jsonl_entries(file_path):
        count = 0
        with open(file_path, 'r') as file:
            for line in file:
                if line.strip():  # Check if the line is not empty after stripping whitespace
                    count += 1
        return count

    result = {}
    for jsonl_fp in tqdm(list(list_jsonl_file(source_path))):
        file_res = {}
        result[jsonl_fp.name] = file_res
        count_jsonl_entries(jsonl_fp)

def iter_and_proc(source_path: Path, functions: dict[str, Callable[[dict], Any]]) -> list[dict[str, Any]]:
    result = []
    for jsonl_fp in tqdm(list(list_jsonl_file(source_path))):
        for jsonl_data in iter_jsonl_file(jsonl_fp):
            da_res = {}
            for func_name, func in functions.items():
                da_res[func_name] = func(jsonl_data)
            result.append(da_res)
            yield da_res
    return result


def filter_by_lang(source: Path, dest: Path, languages: set[str]):
    lang_c = Counter()
    with jsonlines.open(dest, mode='a',) as writer:
        # writer.write(...)
        # with open(dest, "a", encoding="utf-8") as fout:
        for jsonl_data in iter_jsonl_file(source):
            data = jsonl_data["data"]
            try:
                if data.get("referenced_tweets") is None and data.get("lang") in languages:
                    lang_c[jsonl_data["data"]["lang"]] += 1
                    # we cut out
                    writer.write(jsonl_data["data"])
                    other_keys.update(jsonl_data.keys())
            except KeyboardInterrupt as e:
                break
    # print(source.name, lang_c)


def grab_k_dump_to_file(source: Path, k_posts :int= 5, from_sample_n: int=50):
    options = []
    for idx,jsonl_data in enumerate(iter_jsonl_file(source)):
        options.append(jsonl_data)
        if idx == from_sample_n:
            break

    samples =choices(options, k=k_posts)
    return samples

if __name__ == "__main__":
    # with open("data/5_samples.json","w", encoding="utf-8") as fout:
    #     json_s = orjson.dumps(grab_k_dump_to_file(
    #         next(list_jsonl_file(Path("data/twitter-stream-20230113")))
    #     ))
    #     fout.write(json_s.decode("utf-8"))
    # all_langs = count_all_langs(Path("data/twitter-stream-20230113"))
    # print(all_langs)
    # dumped to all_langs.json, total: 3576515

    # orig = count_orig_posts(Path("data/twitter-stream-20230113"))
    # print(orig)
    # > 3576515
    # actually, this is strange... it should be a different number than the lang_dumps
    pass

    # for jsonl_fp in tqdm(list(list_jsonl_file(Path("data/twitter-stream-20230113")))):
    #     print(jsonl_fp)
    #
    # ana_structure(Path("data/twitter-stream-20230113"))

    # testing dynamic calculations...
    langs = ["en", "es", "zxx", "pt"]
    l_c = 0
    orig_c = 0
    for idx, tweet_res in enumerate(iter_and_proc(Path("data/twitter-stream-20230113"),
                  {"type":data_ana_ref, "lang":data_ana_lang})):
        orig_c += 1 if tweet_res["type"] == "original" else 0
        l_c += 1 if tweet_res["lang"] in langs else 0

        print(l_c, orig_c)

    # filter by LANGS, WE STILL DO THAT LATER
    # for jsonl_fp in tqdm(list(list_jsonl_file(Path("data/twitter-stream-20230113")))):
    #     # print(jsonl_fp)
    #     langs = ["en", "es"]
    #     dest_fn = "_".join(langs)
    #     try:
    #         filter_by_lang(jsonl_fp,
    #                        Path(f"lang_filtered/{dest_fn}.jsonl"),
    #                        set(langs))
    #
    #
    #     except KeyboardInterrupt as e:
    #         print(other_keys)
    # print(other_keys)

    # with jsonlines.open("lang_filtered/ok_en_es.jsonl") as reader:
    #     for obj in reader:
    #         print(obj["created_at"])
        # print(len(list(f)))

        # for l in f:
        #     print(json.dumps(json.loads(l), ensure_ascii=False, indent=2))
        #     break
