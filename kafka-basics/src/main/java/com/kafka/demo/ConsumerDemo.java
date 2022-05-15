package com.kafka.demo;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
               log.info("Hello world");
               String bootstrapServers = "127.0.0.1:9092";
               String groupId = "my-kafka application";
               String topic = "first_topic";

              //Create consumer properties
               Properties properties = new Properties();
               properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
               properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
               properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
               properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
               properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


               //Create consumer
               KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

               //subscribe consumer to our topic
                consumer.subscribe(Collections.singletonList(topic));

                //poll for new data
                while(true){
                    log.info("Polling");

                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000)); //wait for 100ms till we get records from kafka else move on and we get empty records

                for(ConsumerRecord<String,String> record:records){
                    log.info("Key: "+record.key()+ ", Value : "+record.value());
                    log.info("Partition:  "+record.partition() + ", Offset: "+record.offset());
                }

                }



    }
}
