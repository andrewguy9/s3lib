# S3Lib

Python library and collection of command line programs for interfacing with S3.
Uses buffering and fixed memory usage, where possible, so that operations on large buckets and objects are safe and easy.

## Installation

`pip install s3lib`

## Configuration
Reads configuration file at `~/.s3`

Config file contents:
```
<AWS ACCESS TOKEN>
<AWS SECRET KEY>
```

## Utilities

```
s3ls
usage: Program lists all the objects in an s3 bucket. Works on really big buckets
       [-h] [--host HOST] [--port PORT] [--output OUTPUT] [--creds CREDS]
       [--mark MARK] [--prefix PREFIX] [--batch BATCH]
       [bucket]
```

```
s3get
usage: Program lists all the objects in an s3 bucket. Works on really big buckets
       [-h] [--host HOST] [--port PORT] [--output OUTPUT] [--creds CREDS]
       [--mark MARK] [--prefix PREFIX] [--batch BATCH]
       bucket key
```

```
s3cp
usage: Program copies an object from one location to another
       [-h] [--host HOST] [--port PORT] [--creds CREDS]
       [--header [HEADERS [HEADERS ...]]]
       src_bucket src_object dst_bucket dst_object
```

```
s3head
usage: Program lists all the objects in an s3 bucket. Works on really big buckets
       [-h] [--host HOST] [--port PORT] [--json] [--creds CREDS]
       bucket objects [objects ...]
```

```
s3put
usage: Program puts an object into s3 
       [-h] [--host HOST] [--port PORT]
       [--creds CREDS]
       [--header [HEADERS [HEADERS ...]]]
       bucket object file
```

```
s3rm
usage: Program deletes s3 keys.
       [-h] [--host HOST] [--port PORT]
       [--creds CREDS] [-v] [--batch BATCH]
       bucket objects [objects ...]
```

```
s3sign
usage: Sign an S3 form.
       [-h] [--creds CREDS] file
```
