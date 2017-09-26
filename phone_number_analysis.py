#!/usr/bin/env Python3

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from tempfile import TemporaryFile

import argparse

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

import requests
from requests_file import FileAdapter

import boto3
import botocore

import re

class PhoneNumbers:
    filt = r"[\(\)\- ]*"
    mid_zero = "(?:{0}\( *?0 *?\))".format(filt)
    phone_regex = "(?:(?<=\D)00{0}3{0}1|\+{0}3{0}1){1}?(?:{0}[0-9]){{9}}".format(filt, mid_zero)
    replace_regex = "{0}|{1}".format(mid_zero, filt)
    zeroplus_regex = "^00"
    phone_nl_filter = re.compile(phone_regex)
    replace_filter = re.compile(replace_regex)
    zeroplus_filter = re.compile(zeroplus_regex)

    output_schema = StructType([
        StructField("num", StringType(), True),
        StructField("urls", ArrayType(StringType()), True)
        ])

    def __init__(self, input_file, output_dir, name, partitions=None):
        self.name = name
        self.input_file = input_file
        self.output_dir = output_dir
        self.partitions = partitions

    def run(self):
        sc = SparkContext(appName=self.name)
        sqlc = SQLContext(sparkContext=sc)

        self.failed_record_parse = sc.accumulator(0)
        self.failed_segment = sc.accumulator(0)

        if self.partitions is None:
            self.partitions = sc.defaultParallelism

        input_data = sc.textFile(self.input_file, minPartitions=self.partitions)
        phone_numbers = input_data.flatMap(self.process_warcs)

        phone_numb_agg_web = phone_numbers.groupByKey().mapValues(list)

        sqlc.createDataFrame(phone_numb_agg_web, schema=self.output_schema) \
                .write \
                .format("parquet") \
                .save(self.output_dir)

        self.log(sc, "Failed segments: {}".format(self.failed_segment.value))
        self.log(sc, "Failed parses: {}".format(self.failed_record_parse.value))

    def log(self, sc, message, level="warn"):
        log = sc._jvm.org.apache.log4j.LogManager.getLogger(self.name) 
        if level == "info":
            log.info(message)
        elif level == "warn":
            log.warn(message)
        else:
            log.warn("Level unknown for logging: {}".format(level))

    def process_warcs(self, input_uri):
        stream = None
        if input_uri.startswith('file:'):
            stream = self.process_file_warc(input_uri)
        elif input_uri.startswith('s3:/'):
            stream = self.process_s3_warc(input_uri)
        if stream is None:
            return []
        return self.process_records(stream)

    def process_s3_warc(self, uri):
        try:
            no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
            s3client = boto3.client('s3', config=no_sign_request)
            s3pattern = re.compile('^s3://([^/]+)/(.+)')
            s3match = s3pattern.match(uri)
            if s3match is None:
                print("Invalid URI: {}".format(uri))
                self.failed_segment.add(1)
                return None
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = TemporaryFile(mode='w+b')
            s3client.download_fileobj(bucketname, path, warctemp)
            warctemp.seek(0)
            return warctemp
        except BaseException as e:
            print("Failed fetching {}\nError: {}".format(uri, e))
            self.failed_segment.add(1)
            return None

    def process_file_warc(self, input_file):
        try:
            return open(input_file[5:], 'rb')
        except BaseException as e:
            print("Error ocurred loading file: {}".format(input_file))
            self.failed_segment.add(1)
            return None
            
    def process_records(self, stream):
        try:
            for rec in ArchiveIterator(stream):
                uri = rec.rec_headers.get_header("WARC-Target-URI")
                if uri is None:
                    continue
                try:
                    for num in self.find_phone_numbers(rec.content_stream()):
                        yield (num, uri)
                except UnicodeDecodeError as e:
                    print("Error: {}".format(e))
                    self.failed_record_parse.add(1)
                    continue

        except BaseException as e:
            print("Failed parsing.\nError: {}".format(e))
            self.failed_segment.add(1)

    def find_phone_numbers(self, content):
        content = content.read().decode('utf-8')
        numbers = self.phone_nl_filter.findall(content)
        nums_filt = {re.sub(self.zeroplus_filter, "+",
                            re.sub(self.replace_filter, "", num))
                     for num in numbers}
        for num in nums_filt:
            yield num 

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Phone number analysis using Apache Spark")
    parser.add_argument("--input", '-i', metavar="segment_file", type=str, required=True,
                        help="uri to input segment file")
    parser.add_argument("--output", '-o', metavar="output_dir", type=str, required=True,
                        help="uri to output directory")
    parser.add_argument("--partitions", '-p', metavar="no_partitions", type=int,
                        help="number of partitions in the input RDD")
    parser.add_argument("--name", '-n', metavar="application_name", type=str, default="Phone Numbers",
                        help="override name of application")
    conf = parser.parse_args()
    pn = PhoneNumbers(conf.input, conf.output,
                      conf.name, partitions=conf.partitions)
    pn.run()
