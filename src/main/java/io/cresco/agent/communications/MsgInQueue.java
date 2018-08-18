package io.cresco.agent.communications;


public class MsgInQueue implements Runnable {


    public void run() {

        while (true) {
            try {

                /*
                if (AgentEngine.MsgInQueueActive) {
                          MsgEvent me = AgentEngine.msgInQueue.take(); //get logevent
                          AgentEngine.msgIn(me);
                } else {
                    Thread.sleep(100);
                }
                */



            } catch (Exception ex) {
                System.out.println("Agent : MsgInQueue Error :" + ex.getMessage());
            }

        }
        //shutdown was called
    }

}