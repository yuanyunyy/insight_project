t(d_iter):
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    CASSANDRA_KEYSPACE = "playground"
    class testTable(Model):
        link_id = columns.Text(primary_key=True)
        url = columns.Text()
        connection.setup(['172.31.6.150'], CASSANDRA_KEYSPACE)
        sync_table(testTable)
        for d in d_iter:
            testTable.create(**d)

# Create table if it does not exist. Need to do this before submitting to Spark to avoid collisions
from pyspark.sql import SQLContext
df = sqlContext.read.json("s3n://reddit-comments/2007/RC_2007-10")
t1 = df.map(lambda r: {"link_id":r.link_id, 
                       "url":"http://www.reddit.com/by_id/" + r.link_id + "/.json"})
test([])
t1.foreachPartition(test)

