package org.wikimedia.producer;

import java.util.Objects;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class WikimediaChangeHandler implements BackgroundEventHandler {
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(
            final KafkaProducer<String, String> kafkaProducer,
            final String topic) {
        this.kafkaProducer = Objects.requireNonNull(kafkaProducer);
        this.topic = Objects.requireNonNull(topic);
    }

    @Override
    public void onOpen() {
        // do nothing
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(
            final String s,
            final MessageEvent messageEvent) {
        logger.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(
            final String s) {
        // do nothing
    }

    @Override
    public void onError(
            final Throwable t) {
        logger.error("Error in stream reading", t);
    }
}
