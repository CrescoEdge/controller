package io.cresco.agent.communications;

import io.micrometer.core.instrument.Timer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;

import javax.jms.*;
import java.util.concurrent.TimeUnit;

public class JMXConsumer {

public JMXConsumer(Timer t) {

    try {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://mybroker");
        factory.setObjectMessageSerializationDefered(true);

        PooledConnectionFactory pfactory = new PooledConnectionFactory();
        pfactory.setConnectionFactory(factory);

        // Create a Connection
        Connection connection = pfactory.createConnection();
        connection.start();

        //connection.setExceptionListener(this);

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue("TEST.FOO");

        // Create a MessageConsumer from the Session to the Topic or Queue
        MessageConsumer consumer = session.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) msg;
                        long startTime = Long.parseLong(textMessage.getText());
                        t.record(System.currentTimeMillis() - startTime,TimeUnit.MILLISECONDS);
                        //String text = textMessage.getText();

                        //System.out.println("Received: " + text);
                    } else {
                        //System.out.println("Received: " + msg);
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        /*
        // Wait for a message
        Message message = consumer.receive(1000);

        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();
            System.out.println("Received: " + text);
        } else {
            System.out.println("Received: " + message);
        }

        consumer.close();
        */

        //session.close();
        //connection.close();


    } catch(Exception ex) {
        ex.printStackTrace();
    }

}
}
