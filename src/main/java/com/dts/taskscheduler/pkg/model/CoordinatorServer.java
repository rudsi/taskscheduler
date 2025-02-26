package com.dts.taskscheduler.pkg.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceImpl;
import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatRequest;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatResponse;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.zaxxer.hikari.HikariDataSource;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;

public class CoordinatorServer {
    private final String serverPort;
    private final Map<Integer, WorkerInfo> workerPool;
    private final ReentrantReadWriteLock workerPoolLock;
    private final int maxHeartBeatMisses;
    private final long heartbeatInterval;
    private final DataSource dbPool;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler;
    private Server grpcServer;
    private final AtomicInteger roundRobinIndex;
    private volatile boolean running;
    private static final Logger logger = Logger.getLogger(CoordinatorServer.class.getName());

    public CoordinatorServer(String serverPort, String dbConnectionString) {
        this.serverPort = serverPort;
        this.workerPool = new ConcurrentHashMap<>();
        this.workerPoolLock = new ReentrantReadWriteLock();
        this.maxHeartBeatMisses = 5;
        this.heartbeatInterval = 5000;
        this.dbPool = DatabaseConnection.connectToDatabase(dbConnectionString);
        this.executorService = Executors.newCachedThreadPool();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.roundRobinIndex = new AtomicInteger(0);
        this.running = true;
    }

    public void startServer() throws IOException {
        grpcServer = ServerBuilder.forPort(Integer.parseInt(serverPort))
                .addService(new CoordinatorServiceImpl(this))
                .build()
                .start();
        manageWorkerPool();
    }

    public void stopServer() {
        running = false;
        scheduler.shutdown();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        workerPoolLock.writeLock().lock();
        try {
            for (WorkerInfo worker : workerPool.values()) {
                if (worker.getGrpcConnection() != null) {
                    worker.getGrpcConnection().shutdown();
                }
            }
            workerPool.clear();
        } finally {
            workerPoolLock.writeLock().unlock();
        }

        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (dbPool instanceof HikariDataSource) {
            ((HikariDataSource) dbPool).close();
        }
    }

    public void awaitTermination() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public WorkerInfo getNextWorker() {
        workerPoolLock.readLock().lock();
        try {
            int workerCount = workerPool.size();
            if (workerCount == 0) {
                return null;
            }

            List<Integer> workerKeys = new ArrayList<>(workerPool.keySet());
            int index = roundRobinIndex.getAndIncrement() % workerCount;
            return workerPool.get(workerKeys.get(index));
        } finally {
            workerPoolLock.readLock().unlock();
        }
    }

    public boolean submitTaskToWorker(TaskRequest task) {
        WorkerInfo worker = getNextWorker();
        if (worker == null) {
            logger.warning("No workers available");
            return false;
        }
        try {
            worker.getWorkerServiceClient().submitTask(task);
            return true;
        } catch (StatusRuntimeException e) {
            logger.warning("Failed to submit task: " + e.getStatus());
            return false;
        }
    }

    public HeartbeatResponse sendHeartbeat(HeartbeatRequest request) {
        int workerId = request.getWorkerId();
        String workerAddress = request.getAddress();

        workerPoolLock.writeLock().lock();
        try {
            if (workerPool.containsKey(workerId)) {
                WorkerInfo worker = workerPool.get(workerId);
                worker.resetHeartbeatMisses();
            } else {
                logger.info("Registering worker: " + workerId);
                ManagedChannel channel = ManagedChannelBuilder.forTarget(workerAddress)
                        .usePlaintext()
                        .build();

                WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient = WorkerServiceGrpc
                        .newBlockingStub(channel);

                WorkerInfo newWorker = new WorkerInfo(workerAddress, channel, workerServiceClient);
                workerPool.put(workerId, newWorker);

                logger.info("Registered worker: " + workerId);
            }
        } catch (StatusRuntimeException e) {
            logger.warning("Error handling heartbeat: " + e.getStatus());
            return HeartbeatResponse.newBuilder().setAcknowledged(false).build();
        } finally {
            workerPoolLock.writeLock().unlock();
        }

        return HeartbeatResponse.newBuilder().setAcknowledged(true).build();
    }

    public void executeAllScheduledTasks() {
        logger.info("Starting scheduled task execution...");

        try (Connection conn = dbPool.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT id, command FROM tasks WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') " +
                            "AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED")) {

                List<TaskRequest> tasks = new ArrayList<>();

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        tasks.add(TaskRequest.newBuilder()
                                .setTaskId(rs.getString("id")) // Set task ID
                                .setData(rs.getString("command")) // Correct field name based on proto
                                .build());
                    }
                }

                for (TaskRequest task : tasks) {
                    if (submitTaskToWorker(task)) { // Ensure task is successfully submitted
                        try (PreparedStatement updateStmt = conn.prepareStatement(
                                "UPDATE tasks SET picked_at = NOW() WHERE id = ?")) {
                            updateStmt.setString(1, task.getTaskId());
                            updateStmt.executeUpdate();
                        }
                    }
                }

                conn.commit(); // Commit only if all operations succeed
                logger.info("Transaction committed successfully.");

            } catch (SQLException e) {
                logger.warning("Rolling back transaction due to error: " + e.getMessage());
                conn.rollback(); // Rollback if any error occurs
            }
        } catch (SQLException e) {
            logger.severe("Database error: " + e.getMessage());
        }
    }

    public void manageWorkerPool() {
        scheduler.scheduleAtFixedRate(this::removeInactiveWorkers, 0, maxHeartBeatMisses * heartbeatInterval,
                TimeUnit.MILLISECONDS);
    }

    public void removeInactiveWorkers() {
        workerPoolLock.writeLock().lock();
        try {
            List<Integer> toRemove = new ArrayList<>();

            for (var entry : workerPool.entrySet()) {
                int workerID = entry.getKey();
                WorkerInfo worker = entry.getValue();

                if (worker.getHeartbeatMisses() > maxHeartBeatMisses) {
                    logger.info("Removing inactive worker: " + workerID);
                    worker.shutdownConnection();
                    ;
                    toRemove.add(workerID);
                } else {
                    worker.incrementHeartbeatMisses();
                }
            }

            for (int workerID : toRemove) {
                workerPool.remove(workerID);
            }
        } finally {
            workerPoolLock.writeLock().unlock();
        }
    }
}
