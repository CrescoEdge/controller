package io.cresco.agent.controller.db.testhelpers;

import io.cresco.library.messaging.MsgEvent;
import java.util.stream.Collectors;

//Did this dirty trick to make sure the relevant parameters for MsgEvent
//objects show up in the test results instead of something like
// "io.cresco.library.MsgEvent@904209348"
class MsgEvent4Test extends io.cresco.library.messaging.MsgEvent {
    private MsgEvent m;
    public MsgEvent4Test(MsgEvent m){
        this.m = m;
    }
    public String toString(){
        return m.getParams().keySet()
                .stream()
                .map((param) -> param+":"+m.getParam(param))
                .collect(Collectors.joining("|"));
    }
    public MsgEvent getMsgEvent(){
        return this.m;
    }
}