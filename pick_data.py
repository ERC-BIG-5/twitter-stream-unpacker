"""
Pick data from the twitter stream dump, by the location index.
(dump-folder, tar-tile, tar-file-member, jsonl-line-index
"""
import bz2
import gzip
import io
import json
import tarfile
from pathlib import Path
from typing import TypeVar, Literal, cast

from sqlalchemy.orm import DeclarativeBase

from consts import CONFIG

# not sure this is needed
T = TypeVar('T', bound=DeclarativeBase)
ZipFormat = Literal["bz2", "gzip"]

def extract_member_in_tar_file(tar_file: Path, member_name: str) -> bytes:
    with tarfile.open(tar_file, 'r') as tar:
        # relevant_members = [member for member in tar.getmembers() if
        #                     (member.name.endswith('.json.bz2') or member.name.endswith('.json.gz'))]
        return tar.extractfile(member_name).read()


def unpack(bytes_data: bytes, compression_type: ZipFormat, name: str):
    if compression_type == "bz2":
        try:
            return bz2.decompress(bytes_data)
        except Exception as e:
            print(f"Error processing {name}: {str(e)}")
    else:
        with gzip.GzipFile(fileobj=io.BytesIO(bytes_data)) as gz_bytes:
            return gz_bytes.read()



def grab_post_from_location(location_index: tuple[str, str, str, int]) -> dict:
    p = CONFIG.STREAM_BASE_FOLDER / f"archiveteam-twitter-stream-{location_index[0]}"
    if not p.exists():
        raise FileNotFoundError(f"{p} does not exist")
    tar_file_path = p / f"twitter-stream-{location_index[1]}.tar"
    if not tar_file_path.exists():
        raise FileNotFoundError(f"{tar_file_path} does not exist")

    compressed_data: bytes = extract_member_in_tar_file(tar_file_path, location_index[2])
    compression_type = Path(location_index[2]).suffix[1:]
    assert compression_type
    jsonl_str_data = unpack(compressed_data, cast(ZipFormat, compression_type), location_index[2])
    lines = jsonl_str_data.decode("utf-8").split("\n")
    # alternatively try bot_suggestions.jsonl_iterator
    # print(jsonl_str_data)
    return json.loads(lines[location_index[3]])


if __name__ == '__main__':
    # thats a test...
    data = grab_post_from_location(("2022-03", "20220301", "20220301/20220301233400.json.gz", 3))
    print(json.dumps(data, indent=2))