# Process & functions

## Timerange entries

`src.create_tine_range_entries`

TimeRangeEvalEntry is an ORM class to store datetime of entries and
their location in the wicked dump-folder/tarfile-jsonl file structure
It stores them in the MAIN db, and also creates statistics (json in the stats folder)


## Generic data-iter

`src.generic_data_iter`

This module runs over the whole dataset (tar files, jsonl files lines) and runs an arbitrary function.
The function can be passed to the main function.

## Create full entries

`src.create_all_entries``

creates full posts (with content) DBPost orm items

## Create annotation entries

`src.create_annon_entries`

`create_annot1__from_time_range_posts(posts: list[dict])`

Creates DBAnnot1Post orm elements in the ANNON db.
main makes use of `from src.collect_from_time_range_table.get_first_tweets_by_hour`, which goes through a time_range table...


## Create Annot from Full db

`src-create_min_db` previous version of annot db (min). depracated since we need k per hour.

## UTIL



