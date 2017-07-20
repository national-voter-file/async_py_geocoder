import os
import boto3
import aiohttp
import asyncio
import asyncpg
import json
import csv
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import zip_longest, takewhile
import time

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger()


# Itertools chunking from standard lib examples
def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def trim_grouper(iterable, n, fillvalue=None):
    for group in grouper(iterable, n, fillvalue=fillvalue):
        yield tuple(takewhile((lambda x: x), group))


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

    db_config = {}
    db_table = None
    # ID and geography col default to id and geom
    id_col = 'id'
    geo_col = 'geom'
    geo_status_col = None

    csv_file = None
    output_file = None
    s3_bucket = None
    es_host = None
    state = None

    sem_count = 50
    conn_limit = 50
    query_limit = 1000

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def run(self):
        sem = asyncio.Semaphore(self.sem_count)
        loop = asyncio.get_event_loop()
        loop.set_debug(enabled=True)
        conn = aiohttp.TCPConnector(limit=self.conn_limit, verify_ssl=False)
        client = aiohttp.ClientSession(connector=conn, loop=loop)
        self.time1 = time.time()
        loop.run_until_complete(self.geocoder_loop(sem, client))

    async def geocoder_loop(self, sem, client):
        """
        Indefinitely loops through the geocoder coroutine, continuing to query
        the database, geocode rows, and update the database with returned values.
        """
        if self.csv_file:
            await self.csv_loop(sem, client)
        else:
            self.query_limit *= 10
            await self.db_loop(sem, client)
        client.close()
        time2 = time.time()
        print('Geocoding took {:2.4f} seconds'.format(time2-self.time1))
        if self.s3_bucket:
            s3 = boto3.resource('s3')
            s3.Object(self.s3_bucket, self.output_file).upload_file(self.output_file)

    async def csv_loop(self, sem, client):
        # Cleaning up CSV output (so that full S3 paths can be used even if local dirs don't exist
        csv_output_file = os.path.join('data', self.output_file.split('/')[-1])
        output_f = open(csv_output_file, 'w')
        fieldnames = [c.lower() for c in self.cols] + ['lat', 'lon']

        writer = csv.DictWriter(output_f, delimiter=',', fieldnames=fieldnames)
        writer.writeheader()
        output_executor = ThreadPoolExecutor(max_workers=8)

        input_f = open(self.csv_file, 'r')
        reader = enumerate(csv.DictReader(input_f, delimiter=','))
        input_executor = ThreadPoolExecutor(max_workers=8)

        csv_slice_gen = trim_grouper(
            (input_executor.submit(self.yield_csv_rows, (i, r)) for i, r in reader),
            self.query_limit
        )

        async with sem:
            for row_slice in csv_slice_gen:
                await asyncio.gather(*(
                    self.handle_update(
                        sem, client, row.result(),
                        executor=output_executor, writer=writer
                    )
                    for row in as_completed(row_slice)
                ))

        input_executor.shutdown()
        input_f.close()
        output_executor.shutdown()
        output_f.close()

    def yield_csv_rows(self, row):
        i, row = row
        row_dict = {'id': i}
        check_cols = self.cols + [c.lower() for c in self.cols]
        for k, v in row.items():
            if k in check_cols:
                row_dict[k.lower()] = v
        return row_dict

    def write_csv_row(self, writer, row):
        writer.writerow(row)

    async def db_loop(self, sem, client):
        pool = await asyncpg.create_pool(**self.db_config)
        async with sem:
            while True:
                addrs_to_geocode = await self.get_unmatched_addresses(pool)
                if not len(addrs_to_geocode):
                    break
                await asyncio.gather(
                    *[self.handle_update(sem, client, row, pool=pool)
                    for row in addrs_to_geocode]
                )

    async def get_unmatched_addresses(self, pool):
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Run the query passing the request argument.
                query_address = 'SELECT {} FROM {} WHERE {} = 1'.format(
                    ', '.join(self.cols), self.db_table, self.geo_status_col
                )
                if self.state:
                    query_address += '\nAND state_name = {}'.format(self.state)
                query_address += '\nLIMIT {}'.format(self.query_limit)

                return await conn.fetch(query_address)

    async def update_address(self, pool, household_id, addr_dict):
        async with pool.acquire() as conn:
            async with conn.transaction():
                if addr_dict:
                    status = 3
                    update_statement = '''
                        UPDATE {}
                        SET
                            {} = ST_SetSRID(ST_MakePoint({}, {}), 4326),
                            {} = {}
                        WHERE {} = {}
                        '''.format(
                            self.db_table,
                            self.geo_col,
                            addr_dict['lon'],
                            addr_dict['lat'],
                            self.geo_status_col,
                            status,
                            self.id_col,
                            household_id
                        )
                else:
                    status = 2
                    update_statement = 'UPDATE {} SET {} = {} WHERE {} = {}'.format(
                        self.db_table, self.geo_status_col, status, self.id_col, household_id
                    )
                await conn.execute(update_statement)

    async def handle_update(self, sem, client, row, **kwargs):
        async with sem:
            u_id, geom = await self.request_geocoder(client, row)
        if u_id:
            if self.csv_file:
                if geom:
                    row.update(geom)
                kwargs['executor'].submit(
                    lambda x: self.write_csv_row(kwargs['writer'], x), row
                )
            else:
                async with sem:
                    await self.update_address(kwargs['pool'], u_id, geom)

    async def request_geocoder(self, client, row):
        """
        Main method that needs to be implemented in subclasses, asynchronously
        requesting a geocoder service.

        Inputs:
            - client: aiohttp.ClientSession for event loop
            - row: Dictionary-like object with the input address data
        """
        raise NotImplementedError('Must implement request_geocoder method')
