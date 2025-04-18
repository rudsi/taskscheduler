package com.dts.taskscheduler.cmd.coordinator;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.dts.taskscheduler.pkg.coordinator.CoordinatorServer;

public class Main {
    public static void main(String[] args) {
        String coordinatorPort = "8080";

        System.out.println("Coordinator Port: " + coordinatorPort);

        String dbConnectionString = DatabaseConnection.getDBConnectionString();

        CoordinatorServer server = CoordinatorServer.newServer(coordinatorPort, dbConnectionString);

        try {
            server.start();
        } catch (Exception e) {
            System.err.println("Server failed to start: " + e.getMessage());
        }
    }
}
