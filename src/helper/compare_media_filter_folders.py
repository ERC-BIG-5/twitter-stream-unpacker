import shutil
from pathlib import Path


def compare_media_filter_folders(orig: Path, filtered: list[Path], destination: Path):
    """
    consider one folder the original
    If a foto is filtered out (not contained) in any of the filtered folders it is excluded from the final set.

    """
    orig_files = [f for f in orig.glob("*")]
    orig_names = set([f.name for f in orig_files])
    filter_out: list[str] = []
    for f in filtered:
        filtered_files = orig_names - set([f.name for f in f.glob("*")])
        filter_out.extend(filtered_files)

    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(exist_ok=True)

    print(filter_out)

    for f in orig_files:
        if f.name not in filter_out:
            shutil.copy(f, destination)


if __name__ == '__main__':
    compare_media_filter_folders(Path("/home/rsoleyma/projects/twitter-stream-unpacker/data/media/2024102000"),
                                 [
                                     Path(
                                         "/home/rsoleyma/projects/twitter-stream-unpacker/data/media/2024102000 (filtered)")
                                 ],
                                 Path("/home/rsoleyma/projects/twitter-stream-unpacker/data/media/2024102000-clear"))
