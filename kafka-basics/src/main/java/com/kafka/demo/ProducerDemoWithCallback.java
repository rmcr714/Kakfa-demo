package com.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
               log.info("Hello world");

               //create producer properties
               Properties properties = new Properties();
               properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
               properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
               properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

               //create the producer
               KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

              //create a producer record
              ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first_topic","hello world");

              //send the data - asynchronous operation
              producer.send(producerRecord, new Callback() {
                  @Override
                  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       //this method is called when record is successfully sent or an exception happens
                      if(e==null){
                          //the record was successfully sent
                          log.info("received new metadata/ \n"+
                                  "Topic "+recordMetadata.topic()+"\n"+
                                  "Partition "+recordMetadata.partition()+"\n"+
                                  "Offset "+recordMetadata.offset()+"\n"+
                                  "TimeStamp "+recordMetadata.timestamp());
                      }else{
                          log.error("Error while producing ",e);
                      }
                  }
              });

              //flush data - synchronous (This causes the program to wait till the data is sent completely by the producer since its asynchronous)
              producer.flush();


              //flush and close producer
              producer.close();


    }
}
