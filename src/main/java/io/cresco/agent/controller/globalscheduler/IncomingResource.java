package io.cresco.agent.controller.globalscheduler;

import io.cresco.library.messaging.MsgEvent;

public interface IncomingResource {
        void incomingMessage(MsgEvent me);
}
