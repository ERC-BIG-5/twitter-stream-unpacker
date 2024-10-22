import json
from pathlib import Path
from random import randint
from time import sleep

from src.consts import BASE_MEDIA_FOLDER
from src.post_filter import download_media


def run(fp: Path):
    folder = create_new_media_group(fp.stem)
    # jsonl-filepath, index-in-file, data, random-index
    data: list[tuple[str, int, dict, int]]
    data = json.load(fp.open(encoding="utf-8"))
    stats: dict[int, list[bool]] = {}

    def get_existing_ids(fp_: Path):
        files = list(fp_.glob("*"))
        return [int(f.stem.split("_")[0]) for f in files]

    done_ids = get_existing_ids(folder)
    print(done_ids)
    for _, _, post, _ in data:
        print(post)
        if post["id"] in done_ids:
            print("skip")
            continue
        byte_array = download_media(post)
        stat = []
        for idx,type_img in enumerate(byte_array):
            if not type_img:
                stat.append(False)
                continue
            file_type, bytes_ = type_img
            img_dest = folder / f'{post["id"]}_{idx}.{file_type}'
            img_dest.write_bytes(bytes_)
            stat.append(file_type)
        stats[post["id"]] = stat
        # if(len(stats) > 10):
        #     break
        sleep(randint(2,6))
    (folder / "stats.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    print(stats)

def create_new_media_group(name: str) -> Path:
    dest = BASE_MEDIA_FOLDER / name
    dest.mkdir(exist_ok=True)
    return dest


if __name__ == '__main__':
    run(Path("/home/rsoleyma/projects/twitter-stream-unpacker/data/time_storage/2024101918.json"))
