package io.cresco.agent.communications;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;

import javax.jms.*;

public class JMXProducer {

public JMXProducer() {

    try {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://mybroker");
        factory.setObjectMessageSerializationDefered(true);

        PooledConnectionFactory pfactory = new PooledConnectionFactory();
        pfactory.setConnectionFactory(factory);

        // Create a Connection
        Connection connection = pfactory.createConnection();
        connection.start();

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue("TEST.FOO");

        // Create a MessageProducer from the Session to the Topic or Queue
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Create a messages
        String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
        TextMessage message = session.createTextMessage(text);

        // Tell the producer to send the message
        System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
        producer.send(message);


        // Clean up
        session.close();
        connection.close();
    } catch(Exception ex) {
        ex.printStackTrace();
    }

}
}
