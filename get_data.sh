#!/usr/bin/env bash

CRAWL=CC-MAIN-2017-13
BASE_URL=https://commoncrawl.s3.amazonaws.com
LOCAL_SEGMENTS=4
FILE_TYPE=wet

local_file_index=input/test_${FILE_TYPE}.txt
s3_sample_file_index=input/test_s3_${FILE_TYPE}.txt

test -d input || mkdir input
test -e $local_files || rm $local_file_index
test -e $s3_sample_file_index || rm $s3_sample_file_index

listing=crawl-data/$CRAWL/$FILE_TYPE.paths.gz
mkdir -p crawl-data/$CRAWL/
wget --timestamping $BASE_URL/$listing -O $listing
gzip -dc $listing | sed 's@^@s3://commoncrawl/@' \
    >input/all_${FILE_TYPE}_$CRAWL.txt

for segment in $(gzip -dc $listing | head -$LOCAL_SEGMENTS ); do
    mkdir -p $(dirname $segment)
    wget --timestamping $BASE_URL/$segment -O $segment
    echo file:$PWD/$segment >> $local_file_index
    echo s3://commoncrawl/$segment >> $s3_sample_file_index
done




