# Twitter Stream Grab processing

this repo processes Twitter Stream grabs from
https://archive.org/details/twitterstream
Each torrent results in a folder for each month
Spritzer collection: 1% of public Stream


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



## file structure

structure:
archive: month (tar.gz file)
    tar files for each day
        many json.bz2 files in a year/month/day/idx structure

## plan
make a status json file,
for logging which archive, day has been processed

```
# FOR EACH DIR IN main.list_dumps
#   main.process_dump
```

process_dump:
# note that 2023_01 has tar files for each day, while 2022_12 has just one tar.gz file

## simple ana
## Notes on language codes

Standard Language Codes

    en: English

        This is the ISO 639-1 two-letter code for the English language.
        It's widely used in various contexts, including web content, software localization, and international communications.
    ja: Japanese
        While not explicitly mentioned in the search results, this is the ISO 639-1 code for Japanese.

Special Language Codes

    und: Undetermined
        This code is used when the language of the content cannot be determined or is not specified.
    zxx: No linguistic content
        This code is used for content that does not actually contain any linguistic material.
        Examples might include purely instrumental music or abstract art.

Non-Standard Codes

    qme: Media English
        This is not a standard ISO language code.
        It may be a custom code used in specific systems or contexts to denote a particular variant of English used in media.
    qam: Amharic
        This is not a standard ISO language code.
        It appears to be a custom or alternative code for Amharic, though the standard ISO 639-1 code for Amharic is "am".

## torrents

https://archive.org/details/twitterstream


https://archive.org/download/archiveteam-twitter-stream-2022-01/archiveteam-twitter-stream-2022-01_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-02/archiveteam-twitter-stream-2022-02_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-03/archiveteam-twitter-stream-2022-03_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-03/archiveteam-twitter-stream-2022-04_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-03/archiveteam-twitter-stream-2022-05_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-06/archiveteam-twitter-stream-2022-06_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-07/archiveteam-twitter-stream-2022-07_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-08/archiveteam-twitter-stream-2022-08_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-09/archiveteam-twitter-stream-2022-09_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-10/archiveteam-twitter-stream-2022-10_archive.torrent
https://archive.org/download/archiveteam-twitter-stream-2022-11/archiveteam-twitter-stream-2022-11_archive.torrent

https://archive.org/download/archiveteam-twitter-stream-2022-12/archiveteam-twitter-stream-2022-12_archive.torrent

## status

2022-12 download but seems corrput
2022-10