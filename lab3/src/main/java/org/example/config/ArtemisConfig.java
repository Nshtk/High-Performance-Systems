package org.example.config;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;

import jakarta.jms.ConnectionFactory;
@Slf4j
@Configuration
@EnableJms
public class ArtemisConfig {
    @Value("${spring.activemq.broker-url}")
    private String brokerUrl;

    @Value("${spring.activemq.user}")
    private String user;

    @Value("${spring.activemq.password}")
    private String password;

    @Bean
    public ConnectionFactory connectionFactory() {
        try {
            log.info(brokerUrl + "" + user );
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
            connectionFactory.setBrokerURL(brokerUrl);
            connectionFactory.setUser(user);
            connectionFactory.setPassword(password);
            connectionFactory.setReconnectAttempts(-1);
            //return connectionFactory;

            // Recommended settings for failover

            connectionFactory.setRetryInterval(1000);
            connectionFactory.setRetryIntervalMultiplier(1.5);
            connectionFactory.setMaxRetryInterval(60000);
            connectionFactory.setReconnectAttempts(-1); // Infinite retries

            return connectionFactory;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ConnectionFactory", e);
        }
    }
/*/

    @Bean
    public ConnectionFactory connectionFactory() throws JMSException {
      //  ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

        // Configure failover
        String brokerUrl = "failover:(tcp://185.159.111.149:61616,tcp://185.159.111.149:61618)" +
                "?ha=true" +
                "&reconnectAttempts=-1" +
                "&retryInterval=1000" +
                "&maxReconnectAttempts=10";

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connectionFactory.setBrokerURL(brokerUrl);
        connectionFactory.setUser("maiadmin");
        connectionFactory.setPassword("eeL1j1C2yZ");

        return connectionFactory;
    }
/*/
    @Bean
    public JmsTemplate jmsTemplate() throws JMSException {
        JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory());
        jmsTemplate.setPubSubDomain(false);
        try {
            Connection connection = connectionFactory().createConnection();
            connection.start();
            connection.close();
            log.info("Successfully connected to Artemis brokers");
        } catch (JMSException e) {
            log.error("Failed to connect to Artemis brokers", e);
            throw new RuntimeException("Connection validation failed", e);
        }

        return jmsTemplate;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() throws JMSException {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory());
        factory.setConcurrency("1-1");
        factory.setPubSubDomain(false);
        return factory;
    }

}