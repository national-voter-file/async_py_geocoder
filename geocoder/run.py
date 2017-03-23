from es_geocoder import ElasticGeocoder
import argparse
import json


parser = argparse.ArgumentParser(description='Geocode script entrypoint')

parser.add_argument('input_file', help='Input file relative to run.py script')
parser.add_argument('-o', '--output_file', dest='output_file', required=False,
                    help='Output file for script, defaults to data/INPUT_output.csv')
parser.add_argument('-s', '--state', dest='state', required=False,
                    help='Two-letter postal code abbreviation to run only one state')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.input_file.endswith('.json'):
        with open(args.input_file, 'r') as f:
            config = json.load(f)
        elastic_geo = ElasticGeocoder(**config)
    elif args.input_file.endswith('.csv'):
        if not args.output_file:
            args.output_file = '.'.join(args.input_file.split('.')[:-1]) + '_output.csv'
        elastic_geo = ElasticGeocoder(
            csv_file=args.input_file,
            output_file=args.output_file
        )
    else:
        raise Exception('Must supply either json or csv input_file')

    elastic_geo.run()
