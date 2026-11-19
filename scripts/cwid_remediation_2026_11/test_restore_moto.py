"""moto round-trip: snapshot -> damage -> --restore puts SS/NS back as sets.
Run: python3 test_restore_moto.py  (needs moto; never touches AWS)"""
import json, os, sys, tempfile
os.environ.update(AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x", AWS_DEFAULT_REGION="us-east-1")
from moto import mock_aws
import boto3
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dynamo_remediation as d

RANGE = {"FeedbackLog": "sk", "ArticleProvenance": "articleId", "PmidProvenance": "pmid", "ExternalArticle": "articleId"}

with mock_aws():
    c = boto3.client("dynamodb", region_name="us-east-1")
    for t in d.SINGLE_ITEM_TABLES + d.MULTI_ITEM_TABLES:
        ks, ad = [{"AttributeName": "uid", "KeyType": "HASH"}], [{"AttributeName": "uid", "AttributeType": "S"}]
        if t in RANGE:
            ks.append({"AttributeName": RANGE[t], "KeyType": "RANGE"})
            ad.append({"AttributeName": RANGE[t], "AttributeType": "N" if t != "FeedbackLog" else "S"})
        c.create_table(TableName=t, KeySchema=ks, AttributeDefinitions=ad, BillingMode="PAY_PER_REQUEST")
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=d.S3_BUCKET)
    c.put_item(TableName="Identity", Item={"uid": {"S": "Ana2037"}, "tags": {"SS": ["a", "b"]}, "n": {"NS": ["1", "2"]}})
    c.put_item(TableName="ArticleProvenance", Item={"uid": {"S": "Ana2037"}, "articleId": {"N": "7"}, "ads": {"SS": ["x", "y"]}})

    ddb = d.Ddb("us-east-1")
    tmp = tempfile.mkdtemp()
    snap = {u: ddb.snapshot_uid(u, tmp) for u in ("Ana2037", "ana2037")}
    path = os.path.join(tmp, "snap.json")
    json.dump({"format": d.SNAPSHOT_FORMAT, "uids": d.snapshot_to_typed(snap)}, open(path, "w"))

    c.delete_item(TableName="Identity", Key={"uid": {"S": "Ana2037"}})
    c.delete_item(TableName="ArticleProvenance", Key={"uid": {"S": "Ana2037"}, "articleId": {"N": "7"}})
    c.put_item(TableName="Identity", Item={"uid": {"S": "ana2037"}})  # canonical created by --apply

    d.restore(ddb, path, ["Ana2037"], True)
    i = c.get_item(TableName="Identity", Key={"uid": {"S": "Ana2037"}})["Item"]
    assert set(i["tags"]["SS"]) == {"a", "b"} and set(i["n"]["NS"]) == {"1", "2"}, i
    p = c.get_item(TableName="ArticleProvenance", Key={"uid": {"S": "Ana2037"}, "articleId": {"N": "7"}})["Item"]
    assert set(p["ads"]["SS"]) == {"x", "y"}, p
    assert "Item" not in c.get_item(TableName="Identity", Key={"uid": {"S": "ana2037"}}), "canonical not removed"
print("restore moto OK")
