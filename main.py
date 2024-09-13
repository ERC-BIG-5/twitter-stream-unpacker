import bz2
import gzip
import io
import json

import tarfile
from pathlib import Path
from typing import Generator
from tqdm import tqdm

from consts import BASE_DATA_PATH, CONFIG, STATUS_FILE, BASE_PROCESS_PATH
from db_config import init_db
import atexit


def iter_dumps() -> list[Path]:
    return CONFIG.STREAM_BASE_FOLDER.glob("*")


def iter_jsonl_files_data(tar_file: Path) -> Generator[tuple[str, bytes], None, None]:
    with tarfile.open(tar_file, 'r') as tar:
        relevant_members = [member for member in tar.getmembers() if
                            (member.name.endswith('.json.bz2') or member.name.endswith('.json.gz'))]
        for member in tqdm(relevant_members):

            extracted_file = tar.extractfile(member)
            if member.name.endswith("bz2"):
                if extracted_file is not None:
                    try:
                        decompressed_data = bz2.decompress(extracted_file.read())
                        yield member.name, decompressed_data
                    except Exception as e:
                        print(f"Error processing {member.name}: {str(e)}")
            else:
                with gzip.GzipFile(fileobj=io.BytesIO(extracted_file.read())) as gz_bytes:
                    yield member.name, gz_bytes.read()



def process_dump(dump_path: Path):
    assert dump_path.name.startswith("archiveteam-twitter-stream")
    dump_file_date = dump_path.name.lstrip("archiveteam-twitter-stream")
    dump_file_status = {}
    status[dump_file_date] = dump_file_status
    dump_dir = BASE_PROCESS_PATH / dump_file_date
    dump_dir.mkdir(exist_ok=True)
    for tar_file in dump_path.glob("twitter-stream-*.tar"):
        tar_date_name = tar_file.name.lstrip("twitter-stream-").rstrip(".tar")
        tar_file_status = {}
        dump_file_status[tar_date_name] = tar_file_status
        for jsonl_file_data in iter_jsonl_files_data(tar_file):
            pass
        break


def iter_tar_files(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.tar")


def list_tar_gz_files(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.tar.gz")



def exit_handler():
    with STATUS_FILE.open("w") as status_file:
        json.dump(status, status_file, indent=2)


def main():
    atexit.register(exit_handler)
    init_db()
    BASE_PROCESS_PATH.mkdir(exist_ok=True)
    for dump in iter_dumps():
        process_dump(dump)


status: dict = {}
if __name__ == "__main__":
    if not STATUS_FILE.exists():
        status = {}
    else:
        status = json.load(open(STATUS_FILE))

    main()

    # process_dump(TORRENT_FOLDER / "archiveteam-twitter-stream-2022-12")
    # for dump in iter_dumps():
    #     process_dump(dump)

    # TODO: We just dumped the 1. file!
    # for tf in list(list_tar_files(Path("/media/ra/hd2/torrents/archiveteam-twitter-stream-2023-01")))[:1]:
    #     print(tf)
    #     for output_path in dump_jsonl_files(tf):
    #         pass
    # print(outpath)
    # break

    # for jsonl_fp in list_jsonl_file(Path("data")):
    #     for idx, jsonl in enumerate(load_jsonl_file(jsonl_fp)):
    #         # print(idx, jsonl)
    #         print(json.dumps(jsonl, indent=2, ensure_ascii=False))
    #         break
