from invoke import task
import diskcache
import requests
import datetime
import sys
import os
from pprint import pprint
import botocore
import json
from collections import Counter
import time
import hashlib
import urllib.parse
import multiprocessing
from queue import Empty
import boto3
import logging
from tqdm import tqdm

# Put the S3 Bucket name here:
BUCKET = "s3_bucket_name"
CACHE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), ".cache")


def get_cache():
    # limiting size of cache to 20GB
    return diskcache.Cache(CACHE_DIR, size_limit=(20 * (1024**3)))


def get_logger():
    logger = logging.getLogger(__name__)
    # Set logging level to logging.WARNING if desired
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(process)d] %(funcName)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_api_key():
    # Retrieves the API key as a secret from S3
    region_name = "us-west-2"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    # Put the secret ID of the API key here:
    response = client.get_secret_value(SecretId="secret_id")

    # If you don't want to use S3, just return the API key
    # as a string here:
    return response["SecretString"]


@task
def print_query_results(context, q):
    url = search_url(q)
    api_key = get_api_key()
    client = boto3.client("s3")
    logger = get_logger()
    sha = hashlib.sha256(url.encode("ascii")).hexdigest()
    query_key = os.path.join("queries/%s.json" % sha)
    if not key_exists(client, query_key):
        results = search(url, api_key, logger)
        if results:
            logger.info("got results for %s" % url)
            query_results = dict(
                query=q, url=url, results=results, timestamp=time.time()
            )
            client.put_object(
                Bucket=BUCKET,
                Key=query_key,
                Body=json.dumps(query_results),
                ContentType="application/json",
                ACL="public-read",
            )

    res = client.get_object(Bucket=BUCKET, Key=query_key)
    pprint(json.loads(res["Body"].read())["results"])


def list_objects_with_metadata(bucket, prefix):
    keys = {}
    s3c = boto3.client("s3")
    continuation_token = None
    while True:
        if continuation_token:
            objects = s3c.list_objects_v2(
                Bucket=bucket, ContinuationToken=continuation_token, Prefix=prefix
            )
        else:
            objects = s3c.list_objects_v2(Bucket=bucket, Prefix=prefix)

        for i in objects.get("Contents", []):
            keys[i["Key"]] = i

        if "NextContinuationToken" in objects:
            continuation_token = objects["NextContinuationToken"]
        else:
            break

    return keys


def key_exists(client, key):
    try:
        client.head_object(Bucket=BUCKET, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise (e)
        else:
            return False


def start_worker(download_queue, response_queue):
    client = boto3.client("s3")
    logger = get_logger()
    cache = get_cache()
    
    dupes = 0

    while True:
        msg = download_queue.get()
        if msg is None:
            break
        image_key = os.path.join(
            "images/%s" % hashlib.sha256(msg.encode("ascii")).hexdigest()
        )
        if key_exists(client, image_key):
            # logger.info("[%s] key exists %s %s" % (os.getpid(), msg, image_key))
            response_queue.put([msg, "success"])
            dupes += 1
            continue
        try:
            res = requests.get(msg, timeout=5)

            if res.status_code == 200:
                if "Content-Type" not in res.headers:
                    # logger.info("missing content-type for url %s" % (msg,))
                    response_queue.put([msg, "error-missingContentType"])
                else:
                    client.put_object(
                        Bucket=BUCKET,
                        Key=image_key,
                        Body=res.content,
                        ACL="public-read",
                        ContentType=res.headers["Content-Type"],
                    )
                    response_queue.put([msg, "success"])
            else:
                response_queue.put([msg, "error-%s" % res.status_code])
        except requests.exceptions.ConnectionError as e:
            response_queue.put([msg, "error-connection"])
        except requests.exceptions.Timeout as e:
            response_queue.put([msg, "error-timeout"])
    print(dupes)

def search(url, api_key, logger):
    res = requests.get(url, headers={"Ocp-Apim-Subscription-Key": api_key})
    if res.status_code == 200:
        return res.json()
    else:
        logger.error(
            "%s failed with status code [%s]: %s" % (url, res.status_code, res.content)
        )
        return None


def search_url(query, image_type="Photo"):
    params = urllib.parse.urlencode(dict(q=query, imageType=image_type))
    return f"https://api.bing.microsoft.com/v7.0/images/search?{params}"


@task
def download_images(context):
    logger = get_logger()
    cache = get_cache()
    client = boto3.client("s3")
    last_query_time = 0

    workers = []
    # if memory becomes an issue then a maxsize will need to be set on the queue
    # setting this to be unlimited assuming the max number of max urls will only be around 2 million
    download_queue = multiprocessing.Queue(0)
    response_queue = multiprocessing.Queue(0)

    total_workers = multiprocessing.cpu_count() * 2
    logger.info("launching %s workers" % total_workers)
    for _ in range(total_workers):
        proc = multiprocessing.Process(
            target=start_worker, args=(download_queue, response_queue)
        )
        proc.start()
        workers.append(proc)

    download_stats = Counter()
    objects = list_objects_with_metadata(BUCKET, "queries")
    logger.info("retrieved total query objects: %s" % len(objects))
    for i, k in enumerate(objects.keys()):
        if i % 10 == 0:
            logger.info(
                "loading queue - total pending: %s " % download_stats["total_urls"]
            )

        data = json.loads(read_with_cache(cache, client, k))

        for result in data["results"]["value"]:
            image_key = os.path.join(
                "images/%s"
                % hashlib.sha256(result["thumbnailUrl"].encode("ascii")).hexdigest()
            )
            download_queue.put(result["thumbnailUrl"])
            download_stats["total_urls"] += 1
            # To get the image itself rather then the thumbnail:
            #download_queue.put(result["contentUrl"])
            #download_stats["total_urls"] += 1

    logger.info("enqueue complete - total urls: %s " % download_stats["total_urls"])

    for p in workers:
        download_queue.put(None)

    while True:
        try:
            url, download_result = response_queue.get(block=True, timeout=5)
            download_stats[download_result] += 1
            download_stats["processed"] += 1

            if download_stats["processed"] % 100 == 0:
                log_download_stats(logger, download_stats)
        except Empty:
            if not workers and response_queue.qsize() == 0:
                break
            new_workers = []
            for p in workers:
                p.join(timeout=0.1)
                if p.exitcode is None:
                    new_workers.append(p)
            workers = new_workers

    log_download_stats(logger, download_stats)


def log_download_stats(logger, download_stats):
    error_count = 0
    for k, v in download_stats.items():
        if k.split("-")[0] == "error":
            error_count += v

    logger.info(
        "Complete: %s%% Success: %s Errors: %s "
        % (
            round(
                float(download_stats["processed"])
                / float(download_stats["total_urls"])
                * 100
            ),
            download_stats["success"],
            error_count,
        )
    )


@task
def query(context, filename):
    api_key = get_api_key()
    logger = get_logger()
    cache = get_cache()
    # cache = diskcache.Cache(CACHE_DIR)
    client = boto3.client("s3")
    last_query_time = 0
    if not os.path.isfile(filename):
        raise ValueError("%s does not exist" % filename)

    with open(filename) as f:
        queries = json.loads(f.read())

    for q in tqdm(queries):
        url = search_url(q)
        sha = hashlib.sha256(url.encode("ascii")).hexdigest()
        query_key = os.path.join("queries/%s.json" % sha)
        if key_exists(client, query_key):
            logger.info("skipping query '%s' - %s exists" % (q, query_key))
            continue

        delta_time = time.time() - last_query_time
        # keep queries to 100 per/sec
        time.sleep(max(0, 0.01 - delta_time))
        results = search(url, api_key, logger)
        last_query_time = time.time()
        if results:
            logger.info("got results for %s" % url)
            query_results = dict(
                query=q, url=url, results=results, timestamp=time.time()
            )
            client.put_object(
                Bucket=BUCKET,
                Key=query_key,
                Body=json.dumps(query_results),
                ContentType="application/json",
                ACL="public-read",
            )


def read_with_cache(cache, client, key):
    if key in cache:
        return cache[key]

    res = client.get_object(Bucket=BUCKET, Key=key)
    data = res["Body"].read()
    cache[key] = data

    return data


@task
def generate_html(context):
    from jinja2 import Template

    cache = get_cache()
    logger = get_logger()
    index_tmpl = Template(open("templates/queries.tmpl").read())
    client = boto3.client("s3")
    objects = list_objects_with_metadata(BUCKET, "queries")
    html_objects = list_objects_with_metadata(BUCKET, "html")
    queries = []
    # if the template is change all the html gets regenerated
    # this could be simplified if the html use javascript to fetch the results
    mod_datetime_results_tmpl = datetime.datetime.fromtimestamp(os.stat("templates/results.tmpl").st_mtime, tz=datetime.timezone.utc)

    for k, metadata in tqdm(objects.items()):
        query_id = os.path.splitext(os.path.basename(k))[0]
        results_key = "html/%s.html" % query_id
        data = json.loads(read_with_cache(cache, client, k))

        queries.append(
            dict(
                query=data["query"],
                imageType="Photo",
                count=len(data["results"]["value"]),
                query_id=query_id,
            )
        )

        if results_key in html_objects:
            last_modified = html_objects[results_key]["LastModified"]
            query_modified = datetime.datetime.fromtimestamp(data["timestamp"], tz=datetime.timezone.utc)

            if last_modified > max(query_modified, mod_datetime_results_tmpl):
                # logger.info("skipping %s - modified date more recent than query and template" % k)
                continue

        logger.info("processing %s - %s" % (k[:16], data["query"] ))

        results_tmpl = Template(open("templates/results.tmpl").read())
        images = []
        for r in data["results"]["value"]:
            images.append(dict(thumbnailUrl=r["thumbnailUrl"]))


        client.put_object(
            Bucket=BUCKET,
            Key=results_key,
            Body=results_tmpl.render(images=images, query=data["query"]),
            ACL="public-read",
            ContentType="text/html",
        )

    client.put_object(
        Bucket=BUCKET,
        Key="html/index.html",
        Body=index_tmpl.render(queries=sorted(queries, key=lambda q: q["query"])),
        ACL="public-read",
        ContentType="text/html",
    )
