package com.dts.taskscheduler.cmd.coordinator;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;

public class Main {

    public static void main(String[] args) {
        String coordinatorPort = System.getProperty("coordinator_port", "8080");
        String dbConnectioString = DatabaseConnection.getDBConnectionString();
        Coordinator
    }

}
