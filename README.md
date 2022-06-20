# Synopsis

Archive and organize continuously dumped AWS Cloudfront access log
files into gzipped data files grouped by year, month, and date. If
your traffic is high enough or doing complex queries, it's probably
better to just write to a database directly.


Before

```text
    S3 log bucket
    |
    +-- <id>.2019-06-21-17.2719299.gz
    +-- <id>.2019-06-21-17.2382979.gz
    +-- <id>.2019-06-21-17.2797113.gz
    +-- <id>.2019-06-21-17.3273923.gz
    +-- ...
    +-- <id>.2019-06-22-17.273h0a4.gz
    +-- <id>.2019-06-22-17.978h0a2.gz
    +-- ...
```

After

```text
    +-- archive-path (local/S3)
        +- year1
        |  +-- month1
        |  |   |-- year1-month1-day1.gz
        |  |   |-- year1-month1-day2.gz
        |  |   +-- year1-month1-day3.gz
        |  +-- month2
        |      |-- year1-month1-day1.gz
        |      |-- year1-month1-day2.gz
        |      +-- year1-month1-day3.gz
        +- year2
           +-- month2
           |   |-- year2-month2-dd1.gz
           |   |-- year2-month2-dd2.gz
           |   +-- year2-month2-dd3.gz
           +-- ...
```

# Requirements

- Python 3.5+
- AWS Boto3

# Usage

## Archive s3 to local drive

```bash
python3 -m awslogparse.s3_to_local --help
```

or

```bash
python3 -m awslogparse.s3_to_local --bucket bucket-name \
     --dbpath path/to/local/archive/dir \
     --bucket-prefix log-prefixes \ # Optional, defaults to ''
     --delete-source              \ # Optional, if invoked, deletes s3 content
     --profile profile-name         # Optional aws named credential
```

where
- `bucket` s3 bucket name where logs are stored
- `dbpath` local db path
- `bucket-prefix` (default='')prefix, if any, of the logs
- `delete-source` if invoked, evalautes to true and deletes files
                  from S3 once processed
- `profile` (default='') AWS named profile. Must have read/delete
            accesst to AWS bucket

Notes:
- Will process only up to 1000 logs on S3 per call
- May overwrite existing data if they have invalid format
  (e.g. manually editted)
- If `delete-source` is not invoked, the script will parse all
  existing keys when ran again


## Archive s3 to s3

```bash
python3 -m examples.s3_to_s3 --help
```
or

```bash
python3 -m examples.s3_to_s3 --inbucket inbound-bucket-name \
    --outbucket outbound-bucket-name \
    --inbucket-prefix inbound-bucket-prefix \   # Optional
    --outbucket-prefix outbound-bucket-prefix \ # Optional
    --delete-source {true,false} \              # Optional, defaults to 'false'
    --profile profile-name                      # optional
```

where
- `inbucket` s3 bucket name where CF logs are dumped
- `outbucket` s3 bucket name where data is merged and archived
- `inbucket-prefix` prefix, if any, of the logs
- `outbucket-prefix` prefix, if any, of the written buckets
- `delete-source` if invoked, deletes files from S3 once processed
- `profile` (default='') AWS named profile. Must have read/delete
            access to AWS bucket

Note:
- Current implementation is not be optimal since it will process only
  up to 1000 logs per call
- May overwrite existing data if they have invalid format
  (e.g. manually editted)

## Simple query

See [examples/query_recent.py](examples/query_recent.py):

```python
t0 = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
t1 = datetime.strftime(datetime.utcnow(), '%Y-%m-%d')

__dirname = os.path.dirname(os.path.realpath(__file__))
# Assuming local archive directory is `./db`
archive_path = os.path.join(__dirname, 'db')

store = DataStoreLocal(archive_path)

# Select 5 columns
# conditions matched using regex
res = store.select(['date', 'time', 'c-ip', 'cs-uri-stem', 'cs(User-Agent)']) \
           .where({'sc-status': '200',
                   'c-ip': '123\.456\.*'}) \
           .daterange([t0, t1]) \
           .execute()

# Explanation:
#  .where(dict) re.regex compatible expression map
#  .daterange([t0, t1]) must be in YYYY-mm-dd format
#  .executre() run the query

res.display()   # Dump results to stdout
```

# Known Limitations

Current implementation does not page AWS keys when listing objects. If
using AWS as the backend store, does not page AWS key results and
fetches only up to 1000 results. If parsing AWS S3 bucket to local
drive, will fetch 1000 keys per call.

# Todo

- Create means to deploy directly to AWS as a cronjob
