# ACM DEBS GC 2014

Usecase implementation in Flink + kafka + Hbase + ElasticSearch + Kibana

## Steps to start the environment

### 1. Start Docker containers

#### Individually using docker run

* Start the main container with flink, kafka, HBase, Zookeeper.

```bash
docker run --name flink_to_hbase -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9092:9092 -p 9095:9095 -p 2181:2181 -p 16000:16000 -p 16010:16010 -p 16020:16020 -p 16030:16030 -p 6123:6123 -p 7203:7203 -p 8081:8081 -p 8090:8090 -e "KAFKA_ADVERTISED_HOST_NAME=192.168.99.100" --hostname hbase-docker --add-host hbase-docker:172.17.0.2 quay.io/koldbyte/flink-cluster
```

* Start the ElasticSearch container

```bash
docker run -p 9200:9200 -p 9300:9300 -e "http.host=0.0.0.0" -e "transport.host=0.0.0.0" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:5.1.2
```

Note: See [Elasticsearch Docker reference](https://www.elastic.co/guide/en/elasticsearch/reference/5.1/docker.html) and Configure vm.max_map_count

* Start the Kibana container

```bash
docker run -p 5601:5601 -e "ELASTICSEARCH_URL=http://192.168.99.100:9200" -e "SERVER_NAME=192.168.99.100" docker.elastic.co/kibana/kibana:5.1.2
```

* Start the container with kafka Manager

```bash
docker run -d \
     -p 9000:9000  \
     -e ZK_HOSTS="192.168.99.100:2181" \
     hlebalbau/kafka-manager:latest \
     -Dpidfile.path=/dev/null
```

#### Docker compose

[TODO] Docker compose version not ready.

```bash
cd docker && docker compose up
```

### 2. Configure kafka manager

Add the 192.168.99.100:2181/kafka as zookeeper cluster in kafka manager

### 3. Copy the data file (CSV compressed in .gz format) to `/data` shared volume

### 4. Setup hbase tables

Open hbase shell and execute the commands from hbase-setup.txt file

### 5. Submit the Smartgrid jar to Flink dashboard at port 8081

### 6. Start the job as below

## Job - Raw data generator

* Class: com.bhaskardivya.projects.smartgrid.job.SensorSourceJob
* Args: --input /data/data.gz --kafkaBroker localhost:9092 --kafkaTopic sensor2

```comment
USAGE:
    SensorSourceJob
    --help\t\t Show this help
    --input <Compressedfile>\t\tURL to the file to read
    --maxServingDelay <0>\t\tAdd a delay to create out-of-order events
    --servingSpeedFactor <1f>\t\t Speed factor against event time
    --kafkaBroker <localhost:9092>\t\t URI to kafka Broker
    --kafkaTopic <sensor_raw>\t\t Topic Name for Kafka
```

## Job - House Averaging & Prediction

* Class: com.bhaskardivya.projects.smartgrid.job.HouseAveragingJob
* Args: --group.id test1 --debug true --topic sensor2

## Job - Plug Averaging & Prediction

* Class: com.bhaskardivya.projects.smartgrid.job.PlugAveragingJob
* Args: --topic sensor2 --group.id test1

## Testing kafka

### Start console producer

`cd /opt/kafka && bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor_raw`

### Start the console producer

`cd /opt/kafka && bin/kafka-console-producer.sh --topic sensor_raw --broker-list localhost:9092`

## Elasticsearch

### Elasticsearch Index

PUT smartgrid/table1min/1
{
    "settings" : {
      "index" : {
          "number_of_shards" : 1, 
          "number_of_replicas" : 1 
      }
  }
}

### Elasticsearch delete indices

`curl -XDELETE '192.168.99.100:9200/smartgrid?pretty'`

### Elasticsearch setup references

* [StackOverflow - Port configuration related](https://stackoverflow.com/questions/44709297/how-can-i-connect-to-elasticsearch-running-in-the-official-docker-image-using-tr)
* [StackOverflow - Max map count](https://stackoverflow.com/questions/41192680/update-max-map-count-for-elasticsearch-docker-container-mac-host#41251595)

```bash
docker-machine ssh
sudo sysctl -w vm.max_map_count=262144
exit
```

### Hbase tuning

```scala
configuration.setLong("hbase.client.scanner.caching", 1000);
```

### Modify /opt/hbase/conf/hbase-site.xml

Note: Already included in main docker

```xml
  <property>
    <name>hbase.zookeeper.property.maxClientCnxns</name>
    <value>0</value>
  </property>
  <property>
    <name>zookeeper.session.timeout</name>
    <value>1200000</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.tickTime</name>
    <value>6000</value>
  </property>
```