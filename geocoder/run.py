import os
import json
import argparse
from es_geocoder import ElasticGeocoder


parser = argparse.ArgumentParser(description='Geocode script entrypoint')

parser.add_argument('input_file', help='Input file relative to run.py script')
parser.add_argument('-o', '--output_file', dest='output_file', required=False,
                    help='Output file for script, defaults to data/INPUT_output.csv')
parser.add_argument('-s', '--state', dest='state', required=False,
                    help='Two-letter postal code abbreviation to run only one state')
parser.add_argument('-b', '--s3_bucket', dest='s3_bucket', required=False,
                    help='Specify S3 bucket if uploading result to S3')
parser.add_argument('-h', '--es_host', dest='es_host', required=False,
                    help='Specify Elasticsearch host', default='elasticsearch')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.input_file.endswith('.json'):
        with open(args.input_file, 'r') as f:
            config = json.load(f)
        elastic_geo = ElasticGeocoder(**config)
    elif args.input_file.endswith('.csv'):
        if not args.output_file:
            args.output_file = '.'.join(args.input_file.split('.')[:-1]) + '_output.csv'
        if not args.s3_bucket:
            elastic_geo = ElasticGeocoder(
                csv_file=args.input_file,
                output_file=args.output_file,
                es_host=args.es_host
            )
        else:
            elastic_geo = ElasticGeocoder(
                csv_file=args.input_file,
                output_file=args.output_file,
                s3_bucket=args.s3_bucket,
                es_host=args.es_host
            )
    else:
        raise Exception('Must supply either json or csv input_file')

    elastic_geo.run()
