def getWeb(source):
    from urllib2 import urlopen, Request
    from pyspark.sql import SQLContext
    import json
    request = Request(source)
    response = urlopen(request)
    data = json.loads(response.read())
    title = data['data']['children'][0]['data']['title']
    # permalink = data['data']['children'][0]['data']['permalink']
    # subreddit = data['data']['children'][0]['data']['subreddit']
    # selftext = data['data']['children'][0]['data']['selftext']
    # subreddit_id = data['data']['children'][0]['data']['subreddit_id']
    # created = data['data']['children'][0]['data']['created']
    # url = data['data']['children'][0]['data']['url']
    # score = data['data']['children'][0]['data']['score']
    return title

def test(d_iter):
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    from cqlengine.query import ModelQuerySet
    CASSANDRA_KEYSPACE = "playground"
    class table1_20150928(Model):
        link_id = columns.Text(primary_key=True)
        comment_id = columns.Text(primary_key=True)
	source = columns.Text()
        title = columns.Text()
	permalink = columns.Text() 
	subreddit = columns.Text()
	subreddit_id = columns.Text()
	selftext = columns.Text()
	created = columns.Text()
	score = columns.Text()
	url = columns.Text()
    connection.setup(['172.31.6.150'], CASSANDRA_KEYSPACE)
    sync_table(table1_20150928)
    for d in d_iter:
        table1_20150928.create(**d)
        

# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
from pyspark.sql import SQLContext
df = sqlContext.read.json("s3n://reddit-comments/2007/RC_2007-10")
t1 = df.map(lambda x: x.link_id).distinct()
t2 = t1.map(lambda x: {"link_id":x.link_id, 
                       "source":"http://www.reddit.com/by_id/" + x.link_id + "/.json",
                       "title":getWeb("http://www.reddit.com/by_id/" + x.link_id + "/.json")})
test([])
t2.foreachPartition(test)
