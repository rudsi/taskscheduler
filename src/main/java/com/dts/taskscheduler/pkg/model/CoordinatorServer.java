package com.dts.taskscheduler.pkg.model;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.sql.DataSource;

import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.Server;

public class CoordinatorServer {

    private final String serverPort;
    private ServerSocket listener;
    private Server grpcServer;
    private ManagedChannel grpcConnection;
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
    private ScheduledExecutorService scheduler;
    private final CountDownLatch shutdownLatch;
    private CompletableFuture<Void> shutdownSignal;

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
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.shutdownLatch = new CountDownLatch(maxHeartbeatMisses);
        this.shutdownSignal = new CompletableFuture<>();
    }

    public void setDbPool(DataSource dbPool) {
        this.dbPool = dbPool;
    }

    public DataSource getDbPool() {
        return this.dbPool;
    }

    public int getMaxHeartbeatMisses() {
        return maxHeartbeatMisses;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public CompletableFuture<Void> getShutdownSignal() {
        return shutdownSignal;
    }

    public void setShutdownSignal(CompletableFuture<Void> shutdownSignal) {
        this.shutdownSignal = shutdownSignal;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public void setScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    public ManagedChannel getGRPCConnection() {
        return grpcConnection;
    }

    public void setGRPCConnection(ManagedChannel grpcConnection) {
        this.grpcConnection = grpcConnection;
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
