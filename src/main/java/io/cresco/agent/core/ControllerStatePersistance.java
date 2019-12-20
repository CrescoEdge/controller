package io.cresco.agent.core;

import io.cresco.library.agent.ControllerMode;

import java.util.Map;

public interface ControllerStatePersistance {

	Map<String, String> getStateMap();
	boolean setControllerState(ControllerMode currentMode, String currentDesc, String globalRegion, String globalAgent, String regionalRegion, String regionalAgent, String localRegion, String localAgent);
}