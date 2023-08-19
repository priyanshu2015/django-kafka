class KafkaTopic:
    """
    Template:
    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 5 --topic <topic_name> --config retention.ms=<retention_time_in_ms>

    Common Retention Period in ms
    -   1 days = 86400000
    -   2 days = 172800000
    -   3 days = 259200000
    -   7 days = 604800000


    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --topic main_producers_kafka_sample_producer --config retention.ms=604800000
    """
    SAMPLE_EVENTS = "main_producers_kafka_sample_producer"