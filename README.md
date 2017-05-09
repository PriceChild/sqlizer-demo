# Sqlizer demo

Basic features:

* Fetches data via http(s) or pushes local files.
* Submits data to Sqlizer api.
  * Chunks data in \<10mb submissions.
  * Monitors progress.
  * Fetches result.
* Inserts data into mysql.

## Requirements
* yum/apt install mysql-client
* pip install -r requirements.txt

## To run...
```
$ ./downloader.py -h
usage: downloader.py [-h] [-u URL] [-f FILENAME] -o OUTPUT [-k API_KEY]
                     [-H MYSQL_HOST] [-U MYSQL_USER] [-P MYSQL_PASS]
                     [-D MYSQL_DB]

Insert Land Registry Data into MySQL

optional arguments:
  -h, --help            show this help message and exit
  -u URL, --url URL     URL to fetch data from
  -f FILENAME, --filename FILENAME
                        file to copy data from
  -o OUTPUT, --output OUTPUT
                        location to save sqlizer output. This should be
                        reviewed before insertion into MySQL
  -k API_KEY, --api-key API_KEY
                        Sqlizer API key. Preferably set using SQLIZER_API_KEY
                        environment variable

MySQL Connection Options:
  -H MYSQL_HOST, --mysql-host MYSQL_HOST
  -U MYSQL_USER, --mysql-user MYSQL_USER
  -P MYSQL_PASS, --mysql-pass MYSQL_PASS
                        Preferably set using SQLIZER_MYSQL_PASSWORD
  -D MYSQL_DB, --mysql-db MYSQL_DB
```
e.g.
```
SQLIZER_PASS="secret1" SQLIZER_API_KEY="secret2" ./downloader.py -u http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2017.csv -H localhost -U sqlizer -D sqlizer -o example.sql
```

## passwords...
Passwords/keys are likely best set via environment variables to hide them from the process listing.

This tool uses the mysql client binary directly. Setting a username/password in a ~/.my.cnf with appropriate permissions is also possible.

## Future improvements?
* Initial data download is held in memory. - Stream it to disk then up to Sqlizer to cope with large dataset?
* Save files to temporary locations by default.
* Supply header row on command line.
* Read options from [config file](https://docs.python.org/3.5/library/configparser.html) in addition to command line.
