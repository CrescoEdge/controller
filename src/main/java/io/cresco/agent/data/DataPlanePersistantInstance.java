package io.cresco.agent.data;

import io.cresco.library.data.TopicType;

import javax.jms.MessageListener;
import java.io.IOException;

public class DataPlanePersistantInstance {

    private TopicType topicType;
    private MessageListener messageListener;
    private String selectorString;

    private String listenerId;

    public DataPlanePersistantInstance(TopicType topicType, MessageListener messageListener, String selectorString, String listenerId) throws IOException {
        this.topicType = topicType;
        this.messageListener = messageListener;
        this.selectorString = selectorString;
        this.listenerId = listenerId;
    }

    public TopicType getTopicType() {
        return topicType;
    }

    public void setTopicType(TopicType topicType) {
        this.topicType = topicType;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public String getSelectorString() {
        return selectorString;
    }

    public void setListenerId(String listenerId) {
        this.listenerId = listenerId;
    }

    public String getListenerId() {
        return listenerId;
    }

    public void setSelectorString(String selectorString) {
        this.selectorString = selectorString;
    }

}
