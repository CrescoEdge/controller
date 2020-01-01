package io.cresco.agent.controller.statemachine;

public interface ControllerSM {
    void start();
    void stop();
    void globalControllerLost(String disc);
    void regionalControllerLost(String disc);
}