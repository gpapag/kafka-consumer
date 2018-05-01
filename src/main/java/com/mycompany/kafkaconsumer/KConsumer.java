package com.mycompany.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

public class KConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(KConsumer.class);
    private static final long POLL_INTERVAL_MS = 100L;

    private final KafkaConsumer<String, String> consumer;

    public KConsumer(List<String> bootstrapServersList, List<String> topicsList)
    {
        Properties properties = new Properties() {{
            put("bootstrap.servers", String.join(",", bootstrapServersList));
            put("auto.offset.reset", "earliest");
            put("enable.auto.commit", "true");
            put("auto.commit.interval.ms", "1000");
            put("group.id", "tester");
            put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }};

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topicsList);
        consumer.seekToBeginning(consumer.assignment());
    }

    public long consume()
    {
        long recordCount = 0L;
        try {
            consumer.poll(0L);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_INTERVAL_MS);
                for (ConsumerRecord<String, String> record : records) {
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    Long currentTime = timestamp.getTime();
                    System.out.printf("Topic[%s] Partition[%d] Offset[%d] KafkaIn[%d] Key[%s] Value[%s] KafkaOut[%d]\n",
                                      record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value(), currentTime);
                    /*
                    logger.debug("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                 record.topic(), record.partition(), record.offset(), record.key(), record.value());
                                 */
                    ++recordCount;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            consumer.close();
        }
        return recordCount;
    }
}
