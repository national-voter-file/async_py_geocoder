from zipfile import ZipFile
import boto3
import sys
import json
import csv
import os
import argparse
import string
from io import BytesIO
import shapefile
import pyproj
from random import SystemRandom
from itertools import chain
from elasticsearch import Elasticsearch, helpers
from rtree import index
from shapely.geometry import Polygon


CURRENT_DIR = os.path.dirname(__file__)

wgs84 = pyproj.Proj('+init=EPSG:4326')
nad83 = pyproj.Proj('+init=EPSG:4269')

s3 = boto3.resource(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
)

parser = argparse.ArgumentParser(description='Elasticsearch census loader')

parser.add_argument('geo_id', help='Geo identifier (State abbrev, FIPS code)')
parser.add_argument('-b', '--s3_bucket', dest='s3_bucket', required=False,
                    help='Specify S3 bucket if uploading result to S3',
                    default='nvf-tiger-2016')
parser.add_argument('-e', '--es_host', dest='es_host', required=False,
                    help='Specify Elasticsearch host', default='elasticsearch')


with open(os.path.join(CURRENT_DIR, 'es', 'census_schema.json'), 'r') as f:
    census_schema = json.load(f)

with open(os.path.join(CURRENT_DIR, 'es', 'synonyms.json'), 'r') as f:
    synonyms = json.load(f)

with open(os.path.join(CURRENT_DIR, 'es', 'fips_state_map.csv'), 'r') as f:
    fips_state_map = {}
    csv_reader = csv.reader(f, delimiter=',')
    for row in csv_reader:
        fips_state_map[row[1]] = row[0]
        fips_state_map[row[0]] = row[1]


tiger_settings = {
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "state_synonyms": {
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "state_synonyms"
            ]
          },
          "address_synonyms": {
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "address_synonyms"
            ]
          }
        },
        "filter": {
          "state_synonyms": {
            "type": "synonym",
            "ignore_case": True,
            "synonyms": synonyms['state_synonyms']
          },
          "address_synonyms": {
            "type": "synonym",
            "ignore_case": True,
            "synonyms": synonyms['address_synonyms']
          }
        }
      }
    }
  },
  "mappings": {
    'addrfeat': census_schema
  }
}


def make_place_rtree(bucket_str, state_str):
    fips_state = fips_state_map[state_str]
    place_obj = s3.Object(bucket_str, 'PLACE/tl_2016_{}_place.zip'.format(fips_state))
    place_bytes = BytesIO(place_obj.get()['Body'].read())
    place_zip = ZipFile(place_bytes)
    place_shp = shapefile.Reader(
        shp=BytesIO(place_zip.read('tl_2016_{}_place.shp'.format(fips_state))),
        dbf=BytesIO(place_zip.read('tl_2016_{}_place.dbf'.format(fips_state)))
    )

    place_idx = index.Index()
    place_map = {}
    name_idx = list(filter(lambda x: x[1][0] == 'NAME', enumerate(place_shp.fields)))[0][0]

    for idx, shape in enumerate(place_shp.shapeRecords()):
        place_map[idx] = {
            'shape': shape,
            'name': shape.record[name_idx],
            'geom': Polygon(shape.shape.points)
        }
        sh = shape.shape.bbox
        place_idx.insert(idx, (sh[1], sh[0], sh[3], sh[2]))
    return place_map, place_idx


def process_zip(obj):
    tiger_key = obj.key[3:-4]
    tiger_bytes = BytesIO(obj.get()['Body'].read())
    zip_tiger = ZipFile(tiger_bytes)
    return shapefile.Reader(
        shp=BytesIO(zip_tiger.read('{}.shp'.format(tiger_key))),
        dbf=BytesIO(zip_tiger.read('{}.dbf'.format(tiger_key)))
    )


def make_bbox_poly(bbox):
    return Polygon([(bbox[0], bbox[1]), (bbox[0], bbox[3]),
                    (bbox[2], bbox[3]), (bbox[2], bbox[1])])


def process_records(reader, place_idx, state_str):
    field_names = [f[0] for f in reader.fields[1:]]
    feature_list = list()

    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        # Getting type error on bytes, converting
        for k in atr:
            if isinstance(atr[k], bytes):
                atr[k] = atr[k].decode('utf-8').strip()

        atr['STATE'] = state_str
        sh = sr.shape.bbox
        for fid in place_idx.intersection([sh[1], sh[0], sh[3], sh[2]]):
            line_box = make_bbox_poly(sh)
            if not line_box.is_valid:
                continue
            if line_box.intersects(place_map[fid]['geom']):
                atr['PLACE'] = place_map[fid]['name']
                break

        geom = sr.shape.__geo_interface__
        geom['coordinates'] = [
            pyproj.transform(nad83, wgs84, p[0], p[1]) for p in geom['coordinates']
        ]

        feature_list.append(dict(type='Feature', geometry=geom, properties=atr))

    return feature_list


if __name__ == '__main__':
    args = parser.parse_args()

    rand_str = ''.join(SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(6))
    index_name = 'tiger-{}'.format(rand_str)

    es = Elasticsearch(host=args.es_host)
    es.indices.create(index=index_name, body=tiger_settings)
    es.indices.put_alias(index=index_name, name='census')

    if args.geo_id.isdigit():
        prefix = args.geo_id
        state_str = fips_state_map[prefix[:2]]
        prefix_str = '{}/tl_2016_{}'.format(prefix[:2], prefix)
    else:
        state_str = args.geo_id.upper()
        prefix = fips_state_map[state_str]
        prefix_str = prefix + '/'

    bucket = s3.Bucket(args.s3_bucket)
    place_map, place_idx = make_place_rtree(args.s3_bucket, state_str)

    # Generator expression for pulling ADDRFEAT data for a single state
    zip_yield = (process_records(process_zip(r), place_idx, state_str)
                 for r in bucket.objects.filter(Prefix=prefix_str))

    # Generator expression unpacking sublist and yielding ES object
    es_gen = ({'_index': index_name,
               '_type': 'addrfeat',
               '_source': f} for sub in zip_yield for f in sub)

    for ok, item in helpers.parallel_bulk(es, es_gen, thread_count=8):
        if not ok:
            print('Error: {}'.format(item))

    print(es.count(index=index_name))
