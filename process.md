# Process & functions

## Timerange entries

`src.create_tine_range_entries`

TimeRangeEvalEntry is an ORM class to store datetime of entries and
their location in the wicked dump-folder/tarfile-jsonl file structure
It stores them in the MAIN db, and also creates statistics (json in the stats folder)


## Generic data-iter

`generic_data_iter`

This module runs over the whole dataset (tar files, jsonl files lines) and runs an arbitrary function.
The function can be passed to the main function.