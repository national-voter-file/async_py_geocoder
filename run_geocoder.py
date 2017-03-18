from es_geocoder import ElasticGeocoder


if __name__ == '__main__':
    elastic_geo = ElasticGeocoder(
        csv_file='data/201605_VRDB_ExtractSAMPLE_OUT.csv',
        output_file='data/wa_sample_output.csv'
        # csv_file='data/vt_sample.csv',
        # output_file='data/vt_sample_output.csv'
    )
    elastic_geo.run()
