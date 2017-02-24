import os
import aiohttp
import asyncio
import asyncpg
import json
import csv


class AsyncGeocoder(object):
    """
    This is the base class for asynchronously geocoding and loading the data
    into a database configured through the docker-compose setup. To run the
    geocoder, just create an instance and then call run().

    The main item that needs to be changed in subclasses is the conn_limit
    property which controls the max amount of HTTP requests at one time. For any
    rate-limited APIs, you'll need to lower it significantly.

    Any properties can be overriden with kwargs, and in any subclass you'll have
    to implement the request_geocoder method returning an awaited tuple with
    household_id and a dictionary structured as GeoJSON geometry.
    """
    cols = [
        'ID',
        'ADDRESS_NUMBER',
        'STREET_NAME',
        'STREET_NAME_POST_TYPE',
        'PLACE_NAME',
        'STATE_NAME',
        'ZIP_CODE'
    ]

    db_config = {
        'host': 'postgis',
        'port': 5432,
        'database': None,
        'user': 'postgres',
        'password': None
    }
    db_table = None
    # ID and geography col default to id and geom
    id_col = 'id'
    geo_col = 'geom'
    geo_status_col = None

    csv_file = None
    output_file = None

    sem_count = 1000
    conn_limit = 200
    query_limit = 10000

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def run(self):
        sem = asyncio.Semaphore(self.sem_count)
        loop = asyncio.get_event_loop()
        conn = aiohttp.TCPConnector(limit=self.conn_limit)
        client = aiohttp.ClientSession(connector=conn, loop=loop)
        loop.run_until_complete(self.geocoder_loop(sem, client))

    async def geocoder_loop(self, sem, client):
        """
        Indefinitely loops through the geocoder coroutine, continuing to query
        the database, geocode rows, and update the database with returned values.
        """
        if self.csv_file:
            await self.csv_loop(sem, client)
        else:
            await self.db_loop(sem, client)
        client.close()

    async def csv_loop(self, sem, client):
        with open(self.csv_file, 'r') as f:
            reader = csv.DictReader(f, delimiter=',')
            addr_items = []
            for i, row in enumerate(reader):
                row_dict = {'id': i}
                for k, v in row.items():
                    if k in self.cols:
                        row_dict[k] = v
                addr_items.append(row_dict)

        async with sem:
            geocoded_addrs = await asyncio.gather(
                *[self.handle_update(client, row) for row in addr_items]
            )

        geocoded_addrs = [a for a in geocoded_addrs if a is not None]
        fieldnames = [c.lower() for c in self.cols] + ['lat', 'lon']

        with open(self.output_file, 'w') as f:
            writer = csv.DictWriter(f, delimiter=',', fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(geocoded_addrs)

    async def db_loop(self, sem, client):
        pool = await asyncpg.create_pool(**self.db_config)
        async with sem:
            while True:
                addrs_to_geocode = await self.get_unmatched_addresses(pool)
                await asyncio.gather(*[self.handle_update(client, row, pool)
                                       for row in addrs_to_geocode])

    async def update_address(self, pool, household_id, addr_dict):
        async with pool.acquire() as conn:
            async with conn.transaction():
                if addr_dict:
                    status = 3
                    update_statement = '''
                        UPDATE {table}
                        SET
                            {geom_col} = ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326),
                            {geo_status_col} = {g_status}
                        WHERE {id_col} = {u_id}
                        '''.format(table=self.db_table,
                                   geom_col=self.geo_col,
                                   lon=addr_dict['lon'],
                                   lat=addr_dict['lat'],
                                   geo_status_col=self.geo_status_col,
                                   g_status=status,
                                   id_col=self.id_col,
                                   u_id=household_id)
                else:
                    status = 2
                    update_statement = '''
                        UPDATE {table}
                        SET {geo_status_col} = {g_status}
                        WHERE {id_col} = {h_id}
                        '''.format(table=self.db_table,
                                   geo_status_col=self.geo_status_col,
                                   g_status=status,
                                   id_col=self.id_col,
                                   h_id=household_id)
                await conn.execute(update_statement)

    async def handle_update(self, client, row, pool=None):
        u_id, geom = await self.request_geocoder(client, row)
        if u_id:
            if self.csv_file:
                if geom:
                    row.update(geom)
                return row
            else:
                await self.update_address(pool, u_id, geom)

    async def request_geocoder(self, client, row):
        """
        Main method that needs to be implemented in subclasses, asynchronously
        requesting a geocoder service.

        Inputs:
            - client: aiohttp.ClientSession for event loop
            - row: Dictionary-like object with the input address data
        """
        raise NotImplementedError('Must implement request_geocoder method')
