#!/usr/bin/env python3

import logging
import argparse
import sys
import requests
import MySQLdb as dbapi2

from os import environ
from os.path import basename
from time import sleep


class SqlizerApi(object):
    def __init__(self, apikey):
        self.apikey = apikey
        self.sqlizer_headers = {'Authorization': 'Bearer %s' % self.apikey}
        self.maxsizebytes = 10000000
        self.uploads = {}

    def step1(self, file_name):
        r = requests.post('https://sqlizer.io/api/files/',
                          headers=self.sqlizer_headers,
                          data={'DatabaseType': 'MySQL',
                                'FileType': 'csv',
                                'FileName': file_name,
                                'TableName': 'table_name',
                                'FileHasHeaders': 'false'
                                }
                          )
        r.raise_for_status()
        upload_id = r.json()['ID']
        self.uploads[upload_id] = 0
        return upload_id

    def really_upload(self, upload_id, content, part_number=1):
        self.uploads[upload_id] += 1
        logging.info("Uploading Part %i" % self.uploads[upload_id])
        r = requests.post('https://sqlizer.io/api/files/%s/data/' % upload_id,
                          headers=self.sqlizer_headers,
                          data={'file': content,
                                'PartNumber': self.uploads[upload_id]})
        logging.info("Completed Part %i" % self.uploads[upload_id])
        r.raise_for_status()

    def finalize(self, upload_id):
        logging.info("Finalising upload")
        r = requests.put('https://sqlizer.io/api/files/%s/' % upload_id,
                         headers=self.sqlizer_headers,
                         data={'Status': 'Uploaded'})
        r.raise_for_status()

    def step2(self, upload_id, content):
        step = 0
        part_number = 1
        while step < len(content):
            self.really_upload(upload_id,
                               content[step:step+self.maxsizebytes],
                               part_number)
            step += self.maxsizebytes
            part_number += 1
        self.finalize(upload_id)

    def upload(self, file_name, content):
        upload_id = self.step1(file_name)
        logging.info("Sqlizer upload ID: %s" % upload_id)
        self.step2(upload_id, content)
        return upload_id

    def get_status(self, upload_id):
        r = requests.get('https://sqlizer.io/api/files/%s/' % upload_id,
                         headers=self.sqlizer_headers)
        r.raise_for_status()
        return r.json()

    def wait_for_processing(self, upload_id):
        while True:
            response = self.get_status(upload_id)
            status = response['Status']
            if status == "Complete":
                logging.info("Conversion Completed")
                return response['ResultUrl']
            elif status == "Failed":
                logging.error("Conversion Failed")
                raise RuntimeError("Conversion Failed")
            else:
                logging.debug("Status: %s" % status)
                sleep(5)

    def get_result(self, result_url):
        # No need to specify authorization header here, it's part of the url.
        r = requests.get(result_url)
        r.raise_for_status()
        return r

    def convert(self, file_name, content):
        upload_id = api.upload(file_name, content)
        result_url = api.wait_for_processing(upload_id)
        return self.get_result(result_url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Insert Land Registry Data into MySQL')
    parser.add_argument('-u', '--url', help='URL to fetch data from')
    parser.add_argument('-f', '--filename', help='file to copy data from')
    parser.add_argument('-o', '--output', required=True, help='location to save sqlizer output. This should be reviewed before insertion into MySQL')
    parser.add_argument('-k', '--api-key', help='Sqlizer API key. Preferably set using SQLIZER_API_KEY environment variable',
                        default=environ.get('SQLIZER_API_KEY'))
    mysql_group = parser.add_argument_group('MySQL Connection Options')
    mysql_group.add_argument('-H', '--mysql-host')
    mysql_group.add_argument('-U', '--mysql-user')
    mysql_group.add_argument('-P', '--mysql-pass', help='Preferably set using SQLIZER_MYSQL_PASSWORD',
                             default=environ.get('SQLIZER_MYSQL_PASSWORD'))
    mysql_group.add_argument('-D', '--mysql-db')
    args = parser.parse_args()

    if not args.api_key:
        raise RuntimeError("Please set SQLIZER_API_KEY environment variable")

    if (args.url and args.filename) or (not args.url and not args.filename):
        print("Please specify one of --filename or --url")
        parser.print_help()
        sys.exit(1)
    elif args.url:
        r = requests.get(args.url)
        r.raise_for_status()
        content = r.text
        file_name = "web_download.csv"
    elif args.filename:
        with open(args.filename) as f:
            content = f.read()
        file_name = basename(args.filename)

    logging.basicConfig(level=logging.DEBUG)

    api = SqlizerApi(args.api_key)
    r = api.convert(file_name, content)

    with open(args.output, 'wb') as f:
        for chunk in r.iter_content(chunk_size=128):
            f.write(chunk)
    if args.mysql_host:
        while True:
            response = input("Please check %s and enter Y to commit." % args.output)
            if response == "Y":
                break
            if response.lower() == "n":
                sys.exit(2)

        db = dbapi2.connect(host=args.mysql_host,
                            database=args.mysql_db,
                            user=args.mysql_user,
                            password=args.mysql_pass)
        with db.cursor() as c:
            c.execute(r.text)
