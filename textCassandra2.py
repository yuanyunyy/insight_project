
m cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
from cqlengine.query import ModelQuerySet
from urllib2 import urlopen, Request
from pyspark.sql import SQLContext
import json


CASSANDRA_KEYSPACE = "playground"
class table4_20150929(Model):
    link_id = columns.Text(primary_key=True)
    source = columns.Text()
    title = columns.Text()
    permalink = columns.Text()
    subreddit = columns.Text()
    subreddit_id = columns.Text()
    selftext = columns.Text()
    created = columns.Float()
    score = columns.Integer()
    url = columns.Text()
connection.setup(['172.31.6.150'], CASSANDRA_KEYSPACE)
sync_table(table4_20150929)

table4_20150929.create(link_id='t3_5z2gi')
table4_20150929.objects(link_id='t3_5z2gi').update(source="Http://www.reddit.com/by_id/t3_5z2gi/.json")
source = "http://www.reddit.com/by_id/t3_5z2gi/.json"
request = Request(source)
response = urlopen(request)
data = json.loads(response.read())
table4_20150929.objects(link_id='t3_5z2gi').update(title = data['data']['children'][0]['data']['title'])
table4_20150929.objects(link_id='t3_5z2gi').update(permalink = data['data']['children'][0]['data']['permalink'])
table4_20150929.objects(link_id='t3_5z2gi').update(subreddit = data['data']['children'][0]['data']['subreddit'])
table4_20150929.objects(link_id='t3_5z2gi').update(selftext = data['data']['children'][0]['data']['selftext'])
table4_20150929.objects(link_id='t3_5z2gi').update(subreddit_id = data['data']['children'][0]['data']['subreddit_id'])
table4_20150929.objects(link_id='t3_5z2gi').update(created = data['data']['children'][0]['data']['created'])
table4_20150929.objects(link_id='t3_5z2gi').update(url = data['data']['children'][0]['data']['url'])
table4_20150929.objects(link_id='t3_5z2gi').update(score = data['data']['children'][0]['data']['score'])

# t.update('http://www.reddit.com/by_id/t3_5z2gi/.json')
