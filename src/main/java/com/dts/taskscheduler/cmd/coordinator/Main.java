package com.dts.taskscheduler.cmd.coordinator;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.dts.taskscheduler.pkg.coordinator.CoordinatorServer;

public class Main {
    public static void main(String[] args) {
        String coordinatorPort = ":8080";

        for (String arg : args) {
            if (arg.startsWith("--coordinator_port=")) {
                coordinatorPort = arg.substring("--coordinator_port=".length());
            }
        }

        if (coordinatorPort.startsWith(":")) {
            coordinatorPort = coordinatorPort.substring(1);
        }

        String dbConnectionString = DatabaseConnection.getDBConnectionString();

        CoordinatorServer server = CoordinatorServer.newServer(coordinatorPort, dbConnectionString);
        try {
            server.start();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
