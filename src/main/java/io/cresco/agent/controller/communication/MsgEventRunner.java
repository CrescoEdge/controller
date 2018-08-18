package io.cresco.agent.controller.communication;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.messaging.MsgEvent;

public class MsgEventRunner implements Runnable {

    private ControllerEngine controllerEngine;
    private MsgEvent me;
    public MsgEventRunner(ControllerEngine controllerEngine, MsgEvent me) {
        this.controllerEngine = controllerEngine;
        this.me = me;
    }

    public void run() {
        controllerEngine.msgIn(me);
    }

}
