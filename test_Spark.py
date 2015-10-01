def test(d_iter):
        from cqlengine import columns
        from cqlengine.models import Model
        from cqlengine.query import ModelQuerySet
        from cqlengine import connection
        from cqlengine.management import sync_table
        from urllib2 import urlopen, Request
        from pyspark.sql import SQLContext
        import json
        from cassandra.cluster import Cluster
        from cassandra.query import SimpleStatement
        import operator
        from sets import Set

        CASSANDRA_KEYSPACE = "playground"
        class table3_timeline(Model):
                link_id = columns.Text(primary_key=True)
                counts = columns.Integer()
                time = columns.Integer(primary_key=True, partition_key=False)
        class table3_comments(Model):
                link_id = columns.Text()
                author = columns.Text()
                body = columns.Text()
                created_utc = columns.Text()
                parent_id = columns.Text()
                subreddit = columns.Text()
                subreddit_id = columns.Text()
                name = columns.Text(primary_key=True)
                score = columns.Integer(index = True)
        class table3_links(Model):
                link_id = columns.Text(primary_key=True)
                title = columns.Text()
                permalink = columns.Text()
                subreddit = columns.Text()
                subreddit_id = columns.Text()
                selftext = columns.Text()
                created = columns.Integer()
                score = columns.Integer()
                url = columns.Text()
                top_comment = columns.Text()
                top_score = columns.Integer()
        connection.setup(['172.31.6.150'], CASSANDRA_KEYSPACE)
        cluster = Cluster(['54.193.123.92'])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        sync_table(table3_links)
        sync_table(table3_comments)
        sync_table(table3_timeline)
        for d in d_iter:
                table3_comments.create(**d)
                input = {}
                createdtime = 0
                obj = table3_links.objects(link_id=d['link_id'])
                cql = "SELECT top_score, created FROM table3_links WHERE link_id='"+d['link_id']+"'"
                stmt = session.execute(cql)
                current = []
                for repo in stmt:
                        current.append(repo)
                if len(current) > 0:
                        createdtime = current[0][1]
                        if int(current[0][0]) < int(d['score']):
                            obj.update(top_comment = d['name'])
                            obj.update(top_score = d['score'])
                else:
                        source = "http://www.reddit.com/by_id/"+d['link_id']+"/.json"
                        request = Request(source)
                        response = urlopen(request)
                        data = json.loads(response.read())
                        input['title'] = data['data']['children'][0]['data']['title']
                        input['permalink'] = data['data']['children'][0]['data']['permalink']
                        input['subreddit'] = data['data']['children'][0]['data']['subreddit']
                        input['selftext'] = data['data']['children'][0]['data']['selftext']
                        input['subreddit_id'] = data['data']['children'][0]['data']['subreddit_id'] 
                        input['created'] = int(data['data']['children'][0]['data']['created'])
                        createdtime = input['created']
                        input['url'] = data['data']['children'][0]['data']['url']
                        input['score'] = data['data']['children'][0]['data']['score']
                        table3_links.create( link_id = d['link_id'],
                                                title = input['title'],
                                                permalink = input['permalink'],
                                                subreddit = input['subreddit'],
                                                selftext = input['selftext'],
                                                subreddit_id = input['subreddit_id'],
                                                created = input['created'],
                                                url = input['url'],
                                                score = input['score'],
                                                top_comment = d['name'],
                                                top_score = d['score'])
                        table3_timeline.create(link_id=d['link_id'], time=0, counts=0)
                timegap = int(abs(int(d['created_utc']) - createdtime)/3600) # one hour
                cql2 = "SELECT counts FROM table3_timeline WHERE link_id='"+d['link_id']+"' AND time=" + str(timegap)
                stmt = session.execute(cql2)
                count_tmp = []
                for rep in stmt:
                        count_tmp.append(rep)
                if len(count_tmp) > 0:
                        timeslot = table3_timeline.objects(link_id=d['link_id'], time=timegap)
                        timeslot.update(counts=(count_tmp[0][0]+1))
                else:
                        table3_timeline.create(link_id=d['link_id'], time=timegap, counts=1)
        sync_table(table3_links)
        sync_table(table3_comments)
        sync_table(table3_timeline)

df = sqlContext.read.json("s3n://yy-data/testJSON.json")
# s3n://reddit-comments/2007/RC_2007-10
rdd = df.map(lambda x: {"link_id": x.link_id, 
                        "author": x.author,
                        "body": x.body,
                        "created_utc": x.created_utc,
                        "parent_id": x.parent_id,
                        "subreddit": x.subreddit,
                        "subreddit_id": x.subreddit_id,
                        "name": x.name,
                        "score": x.score})
test([])
rdd.foreachPartition(test)