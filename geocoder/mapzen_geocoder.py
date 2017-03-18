from async_geocoder import AsyncGeocoder
import asyncio


class MapzenGeocoder(AsyncGeocoder):
    """
    Implements AsyncGeocoder with for Mapzen as a geocoding service. TCP limit
    should be substantially lower than ElasticGeocoder because Mapzen rate
    limits to 6/second.
    """
    sem_count = 5
    conn_limit = 1
    api_key = None
    mapzen_url = 'https://search.mapzen.com/v1/search/structured?api_key='
    # Mapping of columns to desired ones here, substitute other columns names
    # as the new keys, while the values should remain the same
    col_map = {
        'id': 'id',
        'address_number': 'address_number',
        'street_name': 'street_name',
        'street_name_post_type': 'street_name_post_type',
        'place_name': 'place_name',
        'zip_code': 'zip_code',
        'state_name': 'state_name'
    }

    def __init__(self, *args, **kwargs):
        super(MapzenGeocoder, self).__init__(self, *args, **kwargs)

    async def request_geocoder(self, client, row):
        # Replace col names
        for k, v in self.col_map:
            row[v] = row.pop(k, None)

        addr_fields = [row['address_number'],
                       row['street_name'],
                       row['street_name_post_type']]
        addr_str = ' '.join([f for f in addr_fields if f is not None])
        addr_dict = dict(
            address=addr_str,
            locality=row['place_name'],
            region=row['state_name'],
            postal_code=row['zip_code']
        )

        query_url = self.mapzen_url + self.api_key + ''
        for k, v in addr_dict.items():
            if v is not '':
                query_url += '&{}={}'.format(k, v)

        async with client.get(query_url) as response:
            response_json = await response.json()

            # Check if rate limited, if so, pause and pass
            # TODO: Figure out how to handle passing full day quota
            if 'meta' in response_json:
                if response_json['meta']['status_code'] == 429:
                    asyncio.sleep(0.5)
                    return None, None

            if len(response_json['features']) == 0:
                return row['id'], None

            feature = response_json['features'][0]
            if not feature['properties']['accuracy'] == 'point':
                return row['id'], None

            geom_dict = dict(lon=feature['geometry']['coordinates'][0],
                             lat=feature['geometry']['coordinates'][1])

            return row['id'], geom_dict
