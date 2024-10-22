import subprocess
from pathlib import Path
from typing import Union


class RsyncMgmt:

    def __init__(self, remote_agent_host_name: str, remote_loc: str):
        self.remote_agent_host_name = remote_agent_host_name
        self.remote_loc = remote_loc

    def rsync_(self, source_file_folder: Union[Path,str]):
        if isinstance(source_file_folder,str):
            source_file_folder = Path(source_file_folder)
        # rsync -avz --chmod=ugo+r /home/rsoleyma/projects/twitter-stream-unpacker/data/media/* transfer1:/home/bsc/bsc432917/big5/media/
        src_str = source_file_folder.absolute().as_posix()
        # if source_file_folder.is_dir():
        #      src_str += "/*"

        # Construct the rsync command
        rsync_command = [
            'rsync',
            '-az',  # archive mode, and compress
            '--ignore-missing-args',  # ignore missing source files
            '--chmod=ugo+r',
            src_str,
            f'{self.remote_agent_host_name}:{self.remote_loc}'
        ]
        print(rsync_command)
        try:
            # Run the rsync command
            result = subprocess.run(rsync_command, check=True, capture_output=True, text=True)
            print("Rsync completed successfully.")
            print("Output:", result.stdout)
        except subprocess.CalledProcessError as e:
            print("Rsync failed with error:", e)
            print("Error output:", e.stderr)

if __name__ == '__main__':
    mgmt = RsyncMgmt("transfer1","/home/bsc/bsc432917/big5/media/")
    mgmt.rsync_("/home/rsoleyma/projects/twitter-stream-unpacker/data/media/")
