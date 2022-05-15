package com.kafka.demo.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangesHandler implements EventHandler {

    private static Logger log = LoggerFactory.getLogger(WikiMediaChangesHandler.class.getSimpleName());

    KafkaProducer<String,String> producer;
    String topic;

    public  WikiMediaChangesHandler(KafkaProducer<String,String> producer,String topic){
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        //nothing to do here
    }

    @Override
    public void onClosed() throws Exception {
        //when stream closes , so we close the producer
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {

        log.info(messageEvent.getData());
        //send messages asynchronously when receive message event from  sourse
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));

    }

    @Override
    public void onComment(String s) throws Exception {
        //nothing to do here
    }

    @Override
    public void onError(Throwable throwable) {
         log.error("error in stream ",throwable);
    }
}
