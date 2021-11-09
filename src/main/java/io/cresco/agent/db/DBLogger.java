package io.cresco.agent.db;

import java.io.IOException;

public class DBLogger {

    public static java.io.OutputStream disableDerbyLogFile(){
        return new java.io.OutputStream() {
            public void write(int b) throws IOException {
                // Ignore all log messages
            }
        };
    }

}
