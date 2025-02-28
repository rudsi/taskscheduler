package com.dts.taskscheduler.pkg.model;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;

import io.grpc.ManagedChannel;
import io.grpc.Server;

public class WorkerServer {
    private final int id;
    private final String serverPort;
    private final String coordinatorAddress;
    private ServerSocket listener;
    private Server grpcServer;
    private ManagedChannel coordinatorChannel;
    private CoordinatorServiceGrpc.CoordinatorServiceStub coordinatorServiceClient;
    private final Duration heartbeatInterval;
    private final BlockingQueue<TaskRequest> taskQueue;
    private final Map<String, TaskRequest> receivedTasks;
    private final ReentrantLock receivedTasksLock;
    private final ExecutorService executorService;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private ScheduledFuture<?> heartbeatFuture;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean cancelToken = new AtomicBoolean(false);

    public WorkerServer(int id, String serverPort, String coordinatorAddress, Duration heartbeatInterval,
            BlockingQueue<TaskRequest> taskQueue, Map<String, TaskRequest> receivedTasks,
            ReentrantLock receivedTasksLock, ExecutorService executorService) {
        this.id = id;
        this.serverPort = serverPort;
        this.coordinatorAddress = coordinatorAddress;
        this.heartbeatInterval = heartbeatInterval;
        this.taskQueue = taskQueue;
        this.receivedTasks = receivedTasks;
        this.receivedTasksLock = receivedTasksLock;
        this.executorService = executorService;
    }

    public int getId() {
        return id;
    }

    public AtomicBoolean getCancelToken() {
        return cancelToken;
    }

    public void setCancelToken() {
        this.cancelToken.set(true);
    }

    public String getServerPort() {
        return serverPort;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public String getCoordinatorAddress() {
        return coordinatorAddress;
    }

    public ServerSocket getListener() {
        return listener;
    }

    public void setListener(ServerSocket listener) {
        this.listener = listener;
    }

    public Server getGrpcServer() {
        return grpcServer;
    }

    public AtomicInteger getActiveTasks() {
        return activeTasks;
    }

    public void setHeartbeatFuture(ScheduledFuture<?> heartbeatFuture) {
        this.heartbeatFuture = heartbeatFuture;
    }

    public ScheduledFuture<?> getHeartbeatFuture() {
        return heartbeatFuture;
    }

    public void setGrpcServer(Server grpcServer) {
        this.grpcServer = grpcServer;
    }

    public ManagedChannel getCoordinatorChannel() {
        return coordinatorChannel;
    }

    public void setCoordinatorChannel(ManagedChannel coordinatorChannel) {
        this.coordinatorChannel = coordinatorChannel;
    }

    public CoordinatorServiceGrpc.CoordinatorServiceStub getCoordinatorServiceClient() {
        return coordinatorServiceClient;
    }

    public void setCoordinatorServiceClient(CoordinatorServiceGrpc.CoordinatorServiceStub coordinatorServiceClient) {
        this.coordinatorServiceClient = coordinatorServiceClient;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public BlockingQueue<TaskRequest> getTaskQueue() {
        return taskQueue;
    }

    public Map<String, TaskRequest> getReceivedTasks() {
        return receivedTasks;
    }

    public ReentrantLock getReceivedTasksLock() {
        return receivedTasksLock;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
