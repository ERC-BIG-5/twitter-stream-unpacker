import json

import orjson
import label_studio_sdk as ls

from src.labelstudio.labelstudio_client import LabelStudioManager
from src.status import MainStatus


def annotation_status_main():
    main_status = MainStatus.load_status()
    lc_mgmt = LabelStudioManager()
    for ds_key, ds_status in main_status.year_months.items():
        print(ds_key)
        ls_project_ids = ds_status.label_studio_project_ids
        for lang, id in ls_project_ids.items():
            data: ls.Project = lc_mgmt.ls_client.projects.get(id)
            # print(json.dumps(json.loads(orjson.dumps(data.dict())), indent=2))
            print(data.title, data.num_tasks_with_annotations, data.total_annotations_number)


#            lc_mgmt.ls_client.projects.exports
#           pass

if __name__ == '__main__':
    annotation_status_main()
