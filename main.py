import os

import boto3

from flask import Flask, send_file, render_template, redirect
from gestalt import Gestalt

from clearstreet.logger import create_logger


def sanitize_key(key):
    return key.replace("/", "-")


def rebuild_key(key):
    return key.replace("-", "/")


def get_path(key):
    s = key.split("/")
    return "/".join(s[:len(s) - 1])


def get_filename(key):
    s = key.split("/")
    return s[len(s) - 1]


def build_s3_client(config: Gestalt):
    session = boto3.Session()
    access = config.get_string("aws.access_key")
    secret = config.get_string("aws.secret_key")
    url = config.get_string("aws.endpoint_url", 's3.amazonaws.com')
    s3 = session.client(
        service_name="s3",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        endpoint_url=url,
    )
    return s3


def list_buckets(client):
    ret = []
    list = client.list_buckets()['Buckets']
    for bucket in list:
        ret.append(bucket["Name"])
    ret.sort()
    return ret


def list_in_bucket(client, bucket, prefix):
    ret = []
    items = []

    if prefix is None or len(prefix) == 0:
        items = client.list_objects_v2(Bucket=bucket, Delimiter="/")
    else:
        items = client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/")

    for item in items.get('Contents', []):
        if item.get('Key', None) is not None:
            ret.append(item['Key'])
    ret.sort()

    dirs = []
    for item in items.get('CommonPrefixes', []):
        if item.get('Prefix', None) is not None:
            dirs.append(item['Prefix'])

    dirs.sort()
    return ret, dirs


def download_object(client, bucket_name, key):
    key = rebuild_key(key)
    filename = "/tmp" + get_filename(key)
    client.download_file(Bucket=bucket_name, Key=key, Filename=filename)
    return filename


env = os.environ.get('ENV', 'localkube')

logger = create_logger("q1")

app = Flask(__name__)

logger.info("loading configurations", env=env)
g = Gestalt()
g.add_config_file(f'./config/{env}.yaml')
g.build_config()
g.auto_env()

s3client = build_s3_client(g)


@app.route('/download/<bucket>/<path:path>')
def download(bucket, path):
    global s3client
    fname = download_object(s3client, bucket, path)
    return send_file(fname, as_attachment=True)


@app.route('/browse/<bucket>', defaults={'path': ''})
@app.route('/browse/<bucket>/', defaults={'path': ''})
@app.route('/browse/<bucket>/<path:path>')
@app.route('/browse/<bucket>/<path:path>/')
def withinBucket(bucket, path):
    global s3client
    logger.info("inside bucket", bucket=bucket, path=path)
    items, dirs = list_in_bucket(s3client, bucket, path)
    return render_template('browser.html', items=items, dirs=dirs, bucket=bucket, path=path)


@app.route('/browse')
@app.route('/browse/')
def sendhome():
    return redirect("/", code=302)


@app.route('/')
def root():
    global s3client
    buckets = list_buckets(s3client)
    return render_template('index.html', buckets=buckets)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)
