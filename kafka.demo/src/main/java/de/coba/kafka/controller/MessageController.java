package de.coba.kafka.controller;

import java.time.Instant;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.coba.kafka.model.DemoMessage;
import de.coba.kafka.producer.MessageProducer;

@RestController
@RequestMapping("/messages")
public class MessageController {

	private final MessageProducer producer;

	public MessageController(MessageProducer producer) {
		this.producer = producer;
	}

	@PostMapping
	public void send(@RequestBody String text) {
		producer.send(new DemoMessage(text, Instant.now()));
	}

}
