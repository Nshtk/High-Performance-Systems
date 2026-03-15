package org.example.artemis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageConsumer {

    @Value("${app.queues.output}")
    private String outputQueue;

    @JmsListener(destination = "${app.queues.output}")
    public void receiveFromOutputQueue(String message) {
        log.info("Received message from {} queue: {}", outputQueue, message);
        // Process the message here
        log.info("Message processing completed");
    }
}