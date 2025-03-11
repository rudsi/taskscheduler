package com.dts.taskscheduler.cmd.worker;

import com.dts.taskscheduler.pkg.worker.WorkerServer;

public class Main {
    public static void main(String[] args) {
        String workerPort = "8082";
        String coordinatorAddress = "localhost:8080";

        System.out.println("Worker Port: " + workerPort);
        System.out.println("Coordinator Address: " + coordinatorAddress);

        WorkerServer server = WorkerServer.newServer(workerPort, coordinatorAddress);

        try {
            server.start();
        } catch (Exception e) {
            System.err.println("Server failed to start: " + e.getMessage());
        }

    }
}
