# samplepad

There are 3 folders located in this repo called
`gendata`, `totalcounts`, and `storage`.  Each
one represents a data pipeline.  The standard layout
is as follows:

    some-data-pipeline/
    |---configs/
    |   |___specs.yaml
    |---pipelines/
    |   |___some_script.py
    |---Dockerfile
    |---requirements.txt
    |___setup.py

Essentially, it's the containerization of data pipelines.
The `pipelines` folder contains the script which can
be an ETL, EL or, streaming data pipeline.  If you're looking
for some Python code, you can find them in the `pipelines`
folder.

### What's Going On

Right now, we have pushed this repo onto an AWS EC2 
instance.  The 3 data pipelines are build as Docker images 
running as detached / "deamonized" containers at the same 
time on this EC2 instance.

- `gendata` produces made up data about rooms and their 
counts and publishes them to an AWS MSK Kafka Cluster
topic named `rooms-v1`.

- `storage` consumes the raw data using the "EL" pattern 
from the `rooms-v1` topic and loads them onto the S3 bucket
`s3://roomscounts-1b2b3` as parquet files using the
`pyarrow` Python library.

- `totalcounts` consumes the raw data using "ETL" from
topic `rooms-v1` and aggregates up the total overall counts
of each room and the upserts them into a Postgres database 
on AWS RDS.

`gendata` is a producer. `storage` is a consumer part of
the consumer group `coldstorage`.  `totalcounts` is a consumer
part of the consumer group `reckoning`  There's sort-of-a Lambda
Arch going on here...  Sorta.


    AWS EC2 <---> AWS MSK Kafka ---> AWS RDS 
                                ---> AWS S3

### Tech & Software Used

    Languages:  Python SQL
    Tools:  Docker
    Serializations:  Parquet
    Systems:  Kafka Postgres
    AWS Infra:  EC2 MSK RDS S3

### Code Style - DRY vs. Decoupled

There are a few functions repeated in each of the
data pipeline scripts.  The function `config()` is
the same for each one of them.  For initial "dev" purposes,
I'm okay with repeating myself a little.  That way data
pipelines can be tested in isolation.  If a common theme
occurs among data pipelines, then a <b>base image</b>
can be created to hold all common functions / modules.  For
the most part, a OOP framework using something like
polymorphism won't be needed. 

### Code Style - Functional Programming

Hope that the Python code is up to snuff.  It's a little
different than some Python code.  It's PEP 8 for the
most part.  However, having been a Scala coder at one point, 
the Python I write has been heavily influenced by
a lot of functional programming points.  Although I wouldn't
go so far to create Monoids or Monads in Python.  Most likely
not very useful in a Python setting.

### Code Style - Unit vs System Testing

A lot of what data folks do really can't be unit tested.
Most of the functions don't return anything.  Most
of the functions are telling apps / systems to do something.
If they do return anything, they're returning a list or stream
of data.  Unit Tests aren't really meant for that.  
System Tests are better suited for data dealers.  Anyhow,
if anything can be unit tested, it's PathBuilder
found here: https://github.com/WillemRvX/samplepad/blob/main/storage/commons/utils.py#L6.
What do I mean by System Tests.  Well, if you send
a 1,000 pieces of data into something like Kafka, you
better get 1,000 pieces back on the other side, right?

### Random Thoughts on `pyarrow`

Most of my experience with `parquet` has been through Spark.  
Not wanting to spin up AWS EMR because that can be expensive,
used `pyarrow` instead.  First time using it.  Seems
heavily tied to Pandas.  Not too thrilled about that, because
Pandas can really bloat up a Docker image.
Anyhow, was able to figure out how to use it quickly.

### Check it Out if LIVE

If you want to check out the upserts happening in
real time, use this Python script: 
https://github.com/WillemRvX/samplepad/blob/documentation/watch_totcnts_stream.py.
Please `pip install psycopg2` or `pip install psycopg2-binary`.
I'll give you the creds.  Well, assuming I have this up and 
running.