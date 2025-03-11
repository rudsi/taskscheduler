package com.dts.taskscheduler.cmd.scheduler;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.dts.taskscheduler.pkg.scheduler.SchedulerServer;

public class Main {

    public static void main(String[] args) {

        String schedulerPort = "8081";

        System.out.println("Scheduler Port: " + schedulerPort);

        String dbConnectionString = DatabaseConnection.getDBConnectionString();

        SchedulerServer server = SchedulerServer.newServer(schedulerPort, dbConnectionString);

        try {
            server.start();
        } catch (Exception e) {
            System.err.println("Server failed to start: " + e.getMessage());
        }
    }

}
