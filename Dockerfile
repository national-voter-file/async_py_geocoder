FROM python:3.5-onbuild

RUN apt-get update -y && apt-get install -y git libgeos-dev libspatialindex-dev
RUN pip install \
    pyproj==1.9.5.1 \
    aiohttp==1.3.1 \
    asyncpg==0.8.4 \
    boto3==1.4.3 \
    elasticsearch==5.3.0 \
    pyshp==1.2.10 \
    Rtree==0.8.3 \
    shapely==1.6b2

RUN git clone https://github.com/national-voter-file/async_py_geocoder.git /usr/src/async_py_geocoder && \
  mv /usr/src/async_py_geocoder/geocoder/* /usr/src/app

ENTRYPOINT ["python"]
