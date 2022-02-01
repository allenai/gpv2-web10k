# gpv1web-bing

This repository contains the script to download images from the <a href="https://prior.allenai.org/projects/gpv2">Web-10K dataset</a>.
The script takes in a list of queries, queries Bing Image Search, and downloads the returned thumbnail images to an Amazon S3 bucket the user specifies.
To use this script, you will need a <a href="https://www.microsoft.com/en-us/bing/apis/bing-image-search-api">Bing Image Search API</a> key. 

## Setup
```
python3 -mvenv venv
source venv/bin/activate
pip intall -r requirements.txt
```

## Running the script
```
invoke query query_sample.json  # to query Bing Image Search with the queries listed in query_sample.json
invoke print-query-results "mt. everest"  # to print the results of a specific query
invoke generate-html  # to generate an HTML containing the returned images
invoke download-images  # to download the images to an Amazon S3 bucket
```

## Useful links:
<a href="https://www.microsoft.com/en-us/bing/apis/pricing">Bing Image Search API Pricing</a> (for ~40K queries using an S3-tier instance, we paid about $160)

<a href="https://docs.microsoft.com/en-us/bing/search-apis/bing-image-search/reference/query-parameters">Bing Image Search API v7 query parameters</a> (to change the returned response content)

<a href="https://docs.microsoft.com/en-us/bing/search-apis/bing-image-search/reference/response-objects">Bing Image Search APIs v7 response objects</a> (to understand the returned objects)
