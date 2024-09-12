import bz2

import tarfile
from pathlib import Path
from typing import Generator
from tqdm import tqdm

from const import TORRENT_FOLDER, BASE_DATA_DIR


def iter_dumps() ->  list[Path]:
    return TORRENT_FOLDER.glob("*")

def process_dump(dump_path: Path):
    assert dump_path.name.startswith("archiveteam-twitter-stream")
    dump_dir = BASE_DATA_DIR / dump_path.name.lstrip("archiveteam-twitter-stream")
    dump_dir.mkdir(exist_ok=True)
    prepare_dump_structure(dump_path)

def list_tar_files(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.tar")

def list_tar_gz_files(path: Path) -> Generator[Path, None, None]:
    return path.glob("*.tar.gz")

def prepare_dump_structure(dump_path: Path):
    # many tar files?
    tar_files = list(list_tar_files(dump_path))
    if tar_files:
        print("tar-files", dump_path.name)
        # create a folder for each tar_file
        pass
    else:
        print("tar.gz-file", dump_path.name)
        tar_gz_files = list(list_tar_gz_files(dump_path))
        print(tar_gz_files)
        for tar_gz_file in tar_gz_files:
        # there should be one tar.gz file. unzip it
        # Open the tar.gz file
            with tarfile.open(tar_gz_file, 'r:gz') as tar:
                for member in tar.getmembers():
                    pass


def dump_jsonl_files(fp: Path) -> Generator[Path, None, None]:
    dest_folder = Path("data") / fp.stem
    dest_folder.mkdir(exist_ok=True)
    with tarfile.open(fp, 'r') as tar:
        relevant_members = [member for member in tar.getmembers() if member.name.endswith('.json.bz2')]
        for member in tqdm(relevant_members):
            f = tar.extractfile(member)
            if f is not None:
                # Create output path, replacing .json.bz2 with .jsonl
                output_path = (dest_folder / Path(member.name.replace("/", "_")).stem).with_suffix('.jsonl')
                if output_path.exists():
                    continue
                output_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    # Decompress bz2 data
                    decompressed_data = bz2.decompress(f.read())

                    # Write decompressed data to file, handling potential encoding errors
                    with open(output_path, 'wb') as out_file:
                        out_file.write(decompressed_data)

                    # print(f"Extracted and decompressed: {output_path}")
                    yield output_path
                except Exception as e:
                    print(f"Error processing {member.name}: {str(e)}")


if __name__ == "__main__":
    process_dump(TORRENT_FOLDER / "archiveteam-twitter-stream-2022-12")
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