package de.coba.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import de.coba.kafka.model.DemoMessage;

@Component
public class MessageConsumer {

	private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

	@KafkaListener(topics = "spring-topic")
	public void consume(DemoMessage message) {
		log.info("received message: {}", message);
	}

}
