import argparse
import boto3


parser = argparse.ArgumentParser(description='Geocode script entrypoint')

parser.add_argument('es_host', help='Hostname for Elasticsearch service on AWS')
parser.add_argument('-j', '--job', dest='job', required=True,
                    help='Action for Elasticsearch host. Options: create, delete')
parser.add_argument('-a', '--account', dest='account', required=True,
                    help='AWS Account ID')


if __name__ == '__main__':
    args = parser.parse_args()

    es_client = boto3.client('es')

    if args.job == 'create':
        es_client.create_elasticsearch_domain(
            DomainName=args.es_host,
            ElasticsearchVersion='5.3',
            ElasticsearchClusterConfig={
                'InstanceType': 't2.small.elasticsearch',
                'InstanceCount': 3,
                'DedicatedMasterEnabled': False,
                'ZoneAwarenessEnabled': False
            },
            EBSOptions={
                'EBSEnabled': True,
                'VolumeType': 'standard',
                'VolumeSize': 10
            },
            AccessPolicies='arn:aws:iam::{}:role/GeocoderRole'.format(args.account),
            SnapshotOptions={
                'AutomatedSnapshotStartHour': 0
            }
        )
    elif args.job == 'delete':
        es_client.delete_elasticsearch_domain(DomainName=args.es_host)
    else:
        raise ValueError('Must choose create or delete for ES action')
