#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
    
    bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/structured_kafka_wordcount.py localhost:9092 subscribe meu-topico-legal
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import desc
import json
import pyspark.sql.functions as F

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("seiLa")\
        .getOrCreate()


    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # # Split the lines into words

    probes = lines.select(
        split(lines.value,',')[0].alias('timestamp'),
        split(lines.value,',')[1].alias('SSID'),
        split(lines.value,',')[2].alias('Manuf'),
        split(lines.value,',')[3].alias('MAC')
    )

    # probes = probes.select(
    #     'MAC',
    #     "Manuf"
    # ).distinct().groupBy("Manuf").count().sort(desc('count'))

    # probes = probes.select(
    #     F.approxCountDistinct("Manuf")
    # )

    # # Start running the query that prints the running counts to the console
    query = probes\
        .writeStream\
        .option("truncate","false")\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination()
