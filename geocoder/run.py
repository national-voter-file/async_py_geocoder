from es_geocoder import ElasticGeocoder
import argparse


parser = argparse.ArgumentParser(description='Geocode script entrypoint')

parser.add_argument('input_file', help='Input file relative to run.py script')
parser.add_argument('-o', '--output_file', dest='output_file', required=False,
                    help='Output file for script, defaults to data/INPUT_output.csv')


if __name__ == '__main__':
    args = parser.parse_args()
    if not args.output_file:
        args.output_file = '.'.join(args.input_file.split('.')[:-1]) + '_output.csv'

    elastic_geo = ElasticGeocoder(
        csv_file=args.input_file,
        output_file=args.output_file
    )
    elastic_geo.run()
