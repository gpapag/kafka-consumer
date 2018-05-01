package com.mycompany.kafkaconsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaConsumerApp
{
    private KafkaConsumerApp()
    {
    }

    public static void main(String[] args)
    {
        List<String> bootstrapServersList = new ArrayList<String>() {{
            add("198.168.0.1:9092");
            add("198.168.0.2:9092");
            add("198.168.0.3:9092");
            add("198.168.0.4:9092");
        }};

        List<String> topicsList = Collections.singletonList("test");

        KConsumer kafkaConsumer = new KConsumer(bootstrapServersList, topicsList);
        long recordsConsumed = kafkaConsumer.consume();

        System.out.println("Records: " + recordsConsumed);
    }
}
