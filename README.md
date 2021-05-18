# Real-time-Social-Media-Analytics-Tool-for-Twitter
Real-time Social Media Analytics Tool for Twitter building a data platform for real time moderation and analytics of twitter data written in python. The implementation utilize different big data technologies as Spark, Kafka and Hive, in addition to visualization using power bi for data discovery and delivering insights

###
First thing first you have to create a twitter devolper account it may take 3-4 days to get your security tokens that you will need to run the scripts
###

#I deploy this pipeline using hortonworks sandbox hdp 2.6.5 (always make sure to set datetime of the machine correctly)
#The script is running using python 3.6 in vertial environment
### To create vertial environment ###
python3.6 -m venv ./env
source env/bin/activate
pip install --upgrade pip
pip install confluent-kafka
pip install pyspark
pip install tweepy
you will have to install other packages using pip
#I downgrade spark since pyspark 3.1.1 package (pyspark-streaming-kafka) doesn't work 
pip install --force-reinstall pyspark==2.4.6

### Run the producer using ###
source env/bin/activate
python producer.py

### Run the consumer in other shell using ###
source env/bin/activate
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.6  --master consumer.py

### To check on kafka ###

cd /usr/hdp/current/kafka-broker/bin
./kafka-topics.sh --create --zookeeper sandbox-hdp.hortonworks.com:2181 --replication-factor 1 --partitions 1 --topic test
./kafka-console-producer.sh  --broker-list sandbox-hdp.hortonworks.com:6667 --topic test
./kafka-console-consumer.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667 --zookeeper localhost:2181 --topic test
#You don't have to create new topic as the producer will create the topic if it doesn't exist

### Create Hive Table ###
###You have to create a hive table with the same schema (same columns names) to add the data to
create table case_study.test_table1(username string, tweet_id string, tweet_text string, feedback string) STORED AS parquet;

### Snscrape Library ###
#Requirements to use snscrape
Python 3.8 or higher
The Python package dependencies are installed automatically when you install snscrape.
Note that one of the dependencies, lxml, also requires libxml2 and libxslt to be installed.
install the developer version 
pip3 install git+https://github.com/JustAnotherArchivist/snscrape.git
For better understanding to the library check this out
https://github.com/JustAnotherArchivist/snscrape/blob/master/snscrape/modules/twitter.py
