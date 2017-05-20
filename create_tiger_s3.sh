#!/bin/bash
mkdir -p ./TIGER_DATA/PLACE

# Set name of the S3 bucket to be created, the region, and the census year for files
S3_BUCKET=test-nvf-bucket
AWS_REGION=us-east-1
CENSUS_YEAR=2016

FTP_TIGER_URL="ftp://ftp2.census.gov/geo/tiger/TIGER$CENSUS_YEAR/ADDRFEAT/"
HTTP_TIGER_URL="http://www2.census.gov/geo/tiger/TIGER$CENSUS_YEAR/ADDRFEAT/"
FTP_PLACE_URL="ftp://ftp2.census.gov/geo/tiger/TIGER$CENSUS_YEAR/PLACE/"
HTTP_PLACE_URL="http://www2.census.gov/geo/tiger/TIGER$CENSUS_YEAR/PLACE/"

curl -ls $FTP_TIGER_URL 2>&1 | xargs -I {} bash -c 'filename="$1" && wget -P TIGER_DATA/"${filename:8:2}" $HTTP_TIGER_URL$filename' - {}
curl -ls $FTP_PLACE_URL 2>&1 | xargs -I {} wget -P TIGER_DATA/PLACE $HTTP_PLACE_URL{}

aws s3api create-bucket --bucket $S3_BUCKET --region $AWS_REGION
aws s3 sync ./TIGER_DATA s3://$S3_BUCKET
