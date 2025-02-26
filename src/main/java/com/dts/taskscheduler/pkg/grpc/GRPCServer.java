package com.dts.taskscheduler.pkg.grpc;

import java.util.logging.Logger;

import com.dts.taskscheduler.pkg.model.CoordinatorServer;

public class GRPCServer {
    private static final Logger logger = Logger.getLogger(GRPCServer.class.getName());

    private final int port;
    private final CoordinatorServer coordinatorServer;

    public GRPCServer(int port, CoordinatorServer coordinatorServer) {
        this.port = port;
        this.coordinatorServer = coordinatorServer;
    }
}
