package com.dts.taskscheduler.pkg.coordinator;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.sql.DataSource;

import com.dts.taskscheduler.pkg.common.DatabaseConnection;
import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.ClientTaskResponse;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatRequest;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatResponse;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusRequest;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusResponse;
import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc.CoordinatorServiceImplBase;
import com.zaxxer.hikari.HikariDataSource;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CoordinatorServer extends CoordinatorServiceImplBase {
    private volatile boolean running = true;
    private static final long scanInterval = 5000;
    private final String serverPort;
    private ServerSocket listener;
    private Server grpcServer;
    private final Map<Integer, CoordinatorServer.WorkerInfo> workerPool;
    private final ReentrantLock workerPoolMutex;
    private List<Integer> workerPoolKeys;
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

    public CoordinatorServer(String serverPort, String dbConnectionString, int maxHeartbeatMisses,
            Duration heartbeatInterval) {
        this.serverPort = serverPort;
        this.dbConnectionString = dbConnectionString;
        this.maxHeartbeatMisses = maxHeartbeatMisses;
        this.heartbeatInterval = heartbeatInterval;
        this.workerPool = new ConcurrentHashMap<>();
        this.workerPoolMutex = new ReentrantLock();
        this.workerPoolKeys = new ArrayList<>();
        this.workerPoolKeysMutex = new ReentrantReadWriteLock();
        this.roundRobinIndex = new AtomicInteger(0);
        this.executorService = Executors.newCachedThreadPool();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.shutdownLatch = new CountDownLatch(1);
        this.shutdownSignal = new CompletableFuture<>();
    }

    @Override
    public void submitTask(ClientTaskRequest request, StreamObserver<ClientTaskResponse> responseObserver) {
        String data = request.getData();
        String taskId = UUID.randomUUID().toString();
        TaskRequest task = TaskRequest.newBuilder().setTaskId(taskId).setData(data).build();
        try {
            submitTaskToWorker(task);
        } catch (Exception e) {
            responseObserver.onError(e);
            return;
        }

        ClientTaskResponse response = ClientTaskResponse.newBuilder().setMessage("Task submitted successfully")
                .setTaskId(taskId).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    public static CoordinatorServer newServer(String port, String dbConnectionString) {
        int defaultMaxMisses = 3;
        Duration defaultHearbeat = Duration.ofSeconds(5);
        return new CoordinatorServer(port, dbConnectionString, defaultMaxMisses, defaultHearbeat);
    }

    public void start() throws Exception {
        this.executorService.submit(() -> {
            try {
                manageWorkerPool();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        try {
            startGRPCServer();
        } catch (Exception e) {
            throw new Exception("gRPC server failed to start: " + e.getMessage());
        }

        try {
            dbPool = DatabaseConnection.connectToDatabase(dbConnectionString);
        } catch (Exception e) {
            throw e;
        }

        executorService.submit(() -> {
            try {
                scanDatabase();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        awaitShutdown();
    }

    public void startGRPCServer() throws Exception {

        System.out.println("Starting gRPC server on " + serverPort);
        grpcServer = ServerBuilder.forPort(Integer.parseInt(serverPort)).addService(this).build();
        grpcServer.start();

        executorService.submit(() -> {
            try {
                grpcServer.awaitTermination();
            } catch (Exception e) {
                System.err.println("gRPC server interrupted: " + e.getMessage());
            }
        });
    }

    public void awaitShutdown() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Releasing latch.");
            this.shutdownLatch.countDown();
        }));

        this.shutdownLatch.await();
        stop();
    }

    public void stop() throws Exception {
        executorService.shutdownNow();

        workerPoolMutex.lock();

        try {
            for (CoordinatorServer.WorkerInfo worker : workerPool.values()) {
                if (worker.getGRPCConnection() != null) {
                    worker.getGRPCConnection().shutdown();
                }
            }
        } finally {
            workerPoolMutex.unlock();
        }

        if (grpcServer != null) {
            grpcServer.shutdown();
            while (!grpcServer.isTerminated()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (listener != null) {
            listener.close();
        }

        if (dbPool != null) {
            ((HikariDataSource) dbPool).close();
        }
    }

    @Override
    public void updateTaskStatus(UpdateTaskStatusRequest request,
            StreamObserver<UpdateTaskStatusResponse> responseObserver) {
        TaskStatus status = request.getStatus();
        String taskId = request.getTaskId();

        Instant timestamp;
        String column;

        switch (status) {
            case STARTED:
                timestamp = Instant.ofEpochSecond(request.getStartedAt());
                column = "started_at";
                break;
            case COMPLETED:
                timestamp = Instant.ofEpochSecond(request.getCompletedAt());
                column = "completed_at";
                break;
            case FAILED:
                timestamp = Instant.ofEpochSecond(request.getFailedAt());
                column = "failed_at";
                break;
            default:
                System.out.println("Invalid status in UpdateStatusRequest");
                throw new UnsupportedOperationException("Unsupported status: " + status);
        }

        String statement = String.format("UPDATE tasks SET %s = ? WHERE id = ?", column);
        try (Connection conn = dbPool.getConnection();
                PreparedStatement stmt = conn.prepareStatement(statement)) {
            stmt.setTimestamp(1, Timestamp.from(timestamp));
            UUID uuid = UUID.fromString(taskId);
            stmt.setObject(2, uuid);

            int rowsUpdated = stmt.executeUpdate();

            if (rowsUpdated == 0) {
                System.out.println("No rows updated for task: " + taskId);
            }
        } catch (SQLException e) {
            System.err.println(String.format("could not update task status for task %s: %s", taskId, e));
        }

        UpdateTaskStatusResponse response = UpdateTaskStatusResponse.newBuilder().setSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public CoordinatorServer.WorkerInfo getNextWorker() {
        workerPoolKeysMutex.readLock().lock();
        try {
            int workerCount = workerPoolKeys.size();
            System.out.println("worker count: " + workerCount);
            if (workerCount == 0) {
                return null;
            }
            int index = roundRobinIndex.getAndIncrement() % workerCount;
            int key = workerPoolKeys.get(index);
            return workerPool.get(key);
        } finally {
            workerPoolKeysMutex.readLock().unlock();
        }
    }

    public boolean submitTaskToWorker(TaskRequest task) throws Exception {
        CoordinatorServer.WorkerInfo worker = getNextWorker();

        if (worker == null) {
            System.out.println("No workers available");
        }

        try {
            worker.getWorkerServiceClient().submitTask(task);
            return true;
        } catch (StatusRuntimeException e) {
            System.err.println("Error submitting task: " + e.getStatus());
            return false;
        }
    }

    public void manageWorkerPool() {
        scheduler.scheduleAtFixedRate(() -> {
            if (shutdownSignal.isDone()) {
                scheduler.shutdown();
                return;
            }
            removeInactiveWorkers();
        }, 0, maxHeartbeatMisses * heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);

        executorService.submit(() -> {
            try {
                shutdownLatch.await();
                scheduler.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        System.out.println("came here also");
    }

    public void removeInactiveWorkers() {
        workerPoolMutex.lock();

        try {
            Iterator<Map.Entry<Integer, CoordinatorServer.WorkerInfo>> iterator = workerPool.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, CoordinatorServer.WorkerInfo> entry = iterator.next();
                int workerId = entry.getKey();
                CoordinatorServer.WorkerInfo worker = entry.getValue();

                if (worker.getHeartbeatMisses() > maxHeartbeatMisses) {
                    System.out.println("Removing inactive worker: " + workerId);
                    worker.getGRPCConnection().shutdown();
                    iterator.remove();

                    workerPoolKeysMutex.writeLock().lock();
                    try {
                        workerPoolKeys.clear();
                        workerPoolKeys.addAll(workerPool.keySet());
                    } finally {
                        workerPoolKeysMutex.writeLock().unlock();
                    }
                } else {
                    worker.incrementHeartbeatMisses();
                }

            }
        } finally {
            workerPoolMutex.unlock();
        }
    }

    public void scanDatabase() {

        scheduler.scheduleAtFixedRate(() -> {
            if (!running) {
                System.out.println("Shutting down database scanner");
                scheduler.shutdown();
                return;
            }
            executeAllScheduledTasks();
        }, 0, scanInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendHeartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        System.out.println("Received heartbeat request from worker: " + request.getWorkerId());
        workerPoolMutex.lock();
        try {
            int workerId = request.getWorkerId();
            if (workerPool.containsKey(workerId)) {
                WorkerInfo worker = workerPool.get(workerId);
                worker.setHeartbeatMisses(0);
            } else {
                System.out.println("Registering worker: " + workerId);
                ManagedChannel channel = ManagedChannelBuilder.forTarget(request.getAddress()).usePlaintext().build();
                CoordinatorServer.WorkerInfo worker = new WorkerInfo();
                worker.setAddress(request.getAddress());
                worker.setGRPCConnection(channel);
                worker.setWorkerServiceClient(WorkerServiceGrpc.newBlockingStub(channel));
                worker.setHeartbeatMisses(0);
                workerPool.put(workerId, worker);
                workerPoolKeysMutex.writeLock().lock();
                ;
                try {
                    workerPoolKeys = new ArrayList<>(workerPool.keySet());
                } finally {
                    workerPoolKeysMutex.writeLock().unlock();
                }
                System.out.println("Registered worker: " + workerId);
            }
        } finally {
            workerPoolMutex.unlock();
        }
        HeartbeatResponse response = HeartbeatResponse.newBuilder().setAcknowledged(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void executeAllScheduledTasks() {
        final int queryTimeoutSeconds = 30;

        try (Connection conn = dbPool.getConnection()) {
            conn.setAutoCommit(false);
            List<TaskRequest> tasks = new ArrayList<>();
            String query = "SELECT id, command FROM tasks " +
                    "WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') " +
                    "AND picked_at IS NULL " +
                    "ORDER BY scheduled_at " +
                    "FOR UPDATE SKIP LOCKED ";
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String id = rs.getString("id");
                        String command = rs.getString("command");
                        TaskRequest task = TaskRequest.newBuilder().setTaskId(id).setData(command).build();
                        tasks.add(task);
                    }
                }
            } catch (SQLException e) {
                System.out.println("Error executing query: " + e.getMessage());
                conn.rollback();
                return;
            }

            for (TaskRequest task : tasks) {
                try {
                    submitTaskToWorker(task);
                    String updateQuery = "UPDATE tasks SET picked_At = NOW() WHERE id = ?";
                    try (PreparedStatement psUpdate = conn.prepareStatement(updateQuery)) {
                        UUID uuid = UUID.fromString(task.getTaskId());
                        psUpdate.setObject(1, uuid);
                        int rowsUpdated = psUpdate.executeUpdate();
                        if (rowsUpdated == 0) {
                            System.err.println("Failed to update task " + task.getTaskId());
                        }
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                } catch (Exception e) {
                    System.err.println("Error processing task " + task.getTaskId());
                }
            }

            try {
                conn.commit();
            } catch (SQLException e) {
                System.err.println("Failed to commit transaction: " + e.getMessage());
            }

        } catch (SQLException e) {
            System.err.println("Unable to start transaction: " + e.getMessage());
        }

    }

    public static class WorkerInfo {
        private int heartbeatMisses;
        private String address;
        private ManagedChannel grpcConnection;
        private WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient;

        public WorkerInfo() {
        };

        public WorkerInfo(String address, int heartbeatMisses, String host, int port) {
            this.address = address;
            this.heartbeatMisses = heartbeatMisses;
            this.grpcConnection = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            this.workerServiceClient = WorkerServiceGrpc.newBlockingStub(grpcConnection);
        }

        public void incrementHeartbeatMisses() {
            this.heartbeatMisses++;
        }

        public int getHeartbeatMisses() {
            return heartbeatMisses;
        }

        public void setHeartbeatMisses(int heartbeatMisses) {
            this.heartbeatMisses = heartbeatMisses;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public ManagedChannel getGRPCConnection() {
            return grpcConnection;
        }

        public void setGRPCConnection(ManagedChannel grpcConnection) {
            this.grpcConnection = grpcConnection;
        }

        public WorkerServiceGrpc.WorkerServiceBlockingStub getWorkerServiceClient() {
            return workerServiceClient;
        }

        public void setWorkerServiceClient(
                WorkerServiceGrpc.WorkerServiceBlockingStub workerServiceClient) {
            this.workerServiceClient = workerServiceClient;
        }

    }
}
