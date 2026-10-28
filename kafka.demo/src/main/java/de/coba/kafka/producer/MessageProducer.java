package de.coba.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import de.coba.kafka.model.DemoMessage;

@Service
public class MessageProducer {

	private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

	private final KafkaTemplate<String, DemoMessage> kafkaTemplate;

	public MessageProducer(KafkaTemplate<String, DemoMessage> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void send(DemoMessage message) {
		log.info("sending message: {}", message);
		kafkaTemplate.send("spring-topic", message);
	}

}
