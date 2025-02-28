package com.dts.taskscheduler.pkg.model;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.sql.DataSource;

import io.grpc.Server;

public class CoordinatorServer {

    private final String serverPort;
    private ServerSocket listener;
    private Server grpcServer;
    private final Map<Integer, WorkerInfo> workerPool;
    private final ReentrantLock workerPoolMutex;
    private final List<Integer> workerPoolKeys;
    private final ReentrantReadWriteLock workerPoolKeysMutex;
    private final int maxHeartbeatMisses;
    private final Duration heartbeatInterval;
    private final AtomicInteger roundRobinIndex;
    private String dbConnectionString;
    private DataSource dbPool;
    private final ExecutorService executorService;
    private final CountDownLatch shutdownLatch;

    public CoordinatorServer(String serverPort, String dbConnectioString, int maxHeartbeatMisses,
            Duration heartbeatInterval) {
        this.serverPort = serverPort;
        this.dbConnectionString = dbConnectioString;
        this.maxHeartbeatMisses = maxHeartbeatMisses;
        this.heartbeatInterval = heartbeatInterval;
        this.workerPool = new ConcurrentHashMap<>();
        this.workerPoolMutex = new ReentrantLock();
        this.workerPoolKeys = new ArrayList<>();
        this.workerPoolKeysMutex = new ReentrantReadWriteLock();
        this.roundRobinIndex = new AtomicInteger(0);
        this.executorService = Executors.newCachedThreadPool();
        this.shutdownLatch = new CountDownLatch(maxHeartbeatMisses);
    }

    public void setDbPool(DataSource dbPool) {
        this.dbPool = dbPool;
    }

    public DataSource getDbPool() {
        return this.dbPool;
    }

    public void setGRPCServer(Server grpcServer) {
        this.grpcServer = grpcServer;
    }

    public Server getGRPCServer() {
        return this.grpcServer;
    }

    public CountDownLatch getShutdownLatch() {
        return this.shutdownLatch;
    }

    public String getDbConnectionString() {
        return this.dbConnectionString;
    }

    public String getServerPort() {
        return this.serverPort;
    }

    public ServerSocket getListener() {
        return this.listener;
    }

    public Map<Integer, WorkerInfo> getWorkerPool() {
        return this.workerPool;
    }

    public ReentrantLock getWorkerPoolMutex() {
        return this.workerPoolMutex;
    }

    public List<Integer> getWorkerPoolKeys() {
        return this.workerPoolKeys;
    }

    public ReentrantReadWriteLock getWorkerPoolKeysMutex() {
        return this.workerPoolKeysMutex;
    }

    public AtomicInteger getRoundRobinIndex() {
        return this.roundRobinIndex;
    }

}
