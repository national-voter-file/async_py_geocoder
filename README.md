# Asyncio Geocoder

Load data from TIGER ADDRFEAT address ranges into Elasticsearch for fast geocoding
using address interpolation and Python 3's `asyncio`, as well as `docker-compose`
to make running the setup easier.

## Load ADDRFEAT Data

The Elasticsearch TIGER loading script reads directly from S3 into memory, but because
it uses `boto3`, you'll need valid AWS credentials set up through `aws-cli` or
exposed on your machine as environment variables. The operations we're running
won't have a cost for anyone reading the data, but `boto3` requires valid credentials.

Currently, there's an S3 bucket with this information ([viewer here](https://nvf-tiger-2016.s3.amazonaws.com/index.html)),
but you can create one with a similar setup (SCRIPT PENDING).

To run locally, just run:

```
docker-compose build
docker-compose up
```

### ES TIGER Data Loading

If you've successfully built and started the containers as shown above, you should
be able to start loading TIGER data with:

`docker-compose run geocoder python /geocoder/es_tiger_loader.py WA`

Where `WA` is the two letter state abbreviation for the state you want to load
TIGER data from.

The [`grasshopper-loader`](https://github.com/cfpb/grasshopper-loader) repository
will work for loading address point data (following their instructions for `index.js`),
and you can clone it into the `geocoder` directory and it will be ignored by git.
Uncomment the `loader` section of the `docker-compose` file, and run:

`docker-compose run loader ./index.js -h elasticsearch -f ./data.json`

### Running the Geocoder

To start the geocoder itself (which can run into issues if it's started at
the same time as the other containers), run
`docker-compose run geocoder python run_geocoder.py`
