package com.dts.taskscheduler.pkg.worker;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.HeartbeatRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.grpc.Api.UpdateTaskStatusRequest;
import com.dts.taskscheduler.pkg.grpc.WorkerServiceGrpc.WorkerServiceImplBase;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class WorkerServer extends WorkerServiceImplBase {
    private static final Logger logger = Logger.getLogger(WorkerServer.class.getName());
    private static final Duration DEFAULT_HEARTBEAT = Duration.ofSeconds(10);
    private static final int workerPoolSize = 5;
    private final Duration taskProcessTime = Duration.ofSeconds(5);

    private final int id;
    private String serverPort;
    private final String coordinatorAddress;
    private ServerSocket listener;
    private Server grpcServer;
    private ManagedChannel grpcConnection;
    private CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinatorServiceClient;
    private final Duration heartbeatInterval;
    private final BlockingQueue<TaskRequest> taskQueue;
    private final ExecutorService executorService;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private ScheduledFuture<?> heartbeatFuture;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean cancelToken = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch;

    public WorkerServer(int id, String serverPort, String coordinatorAddress, Duration heartbeatInterval,
            BlockingQueue<TaskRequest> taskQueue, Map<String, TaskRequest> receivedTasks,
            ReentrantLock receivedTasksLock, ExecutorService executorService, CountDownLatch shutDownLatch) {
        this.id = id;
        this.serverPort = serverPort;
        this.coordinatorAddress = coordinatorAddress;
        this.heartbeatInterval = heartbeatInterval;
        this.taskQueue = taskQueue;
        this.executorService = executorService;
        this.shutdownLatch = shutDownLatch;
    }

    public WorkerServer newServer(String port, String coordinator) {
        return new WorkerServer(
                generateUniqueId(),
                port,
                coordinator,
                DEFAULT_HEARTBEAT,
                new ArrayBlockingQueue<>(100),
                new ConcurrentHashMap<>(),
                new ReentrantLock(),
                Executors.newCachedThreadPool(),
                new CountDownLatch(1));
    }

    private static int generateUniqueId() {
        return Math.abs(UUID.randomUUID().hashCode());
    }

    public void start(WorkerServer server) throws Exception {
        startWorkerPool(server, workerPoolSize);

        try {
            periodicHeartbeat();
            startGRPCServer();
            awaitShutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            closeGRPCConnection();
        }
        awaitShutdown();
    }

    public boolean connectToCoordinator(WorkerServer server) throws Exception {
        System.out.println("Connecting to coordinator...");

        try {
            grpcConnection = ManagedChannelBuilder.forTarget(coordinatorAddress).usePlaintext().build();
            coordinatorServiceClient = CoordinatorServiceGrpc.newBlockingStub(grpcConnection);
            logger.info("Connected to coordinator!");
            return true;
        } catch (Exception e) {
            logger.severe("Failed to connect to coordinator: " + e.getMessage());
            return false;
        }

    }

    public void periodicHeartbeat() {
        activeTasks.incrementAndGet();

        Runnable heartbeatTask = () -> {
            try {
                if (!sendHeartbeat()) {
                    System.out.println("Failed to send heartbeat");
                }
            } catch (Exception e) {
                System.err.println("Exception during heartbeat: " + e.getMessage());
            }
        };

        heartbeatFuture = scheduler.scheduleAtFixedRate(
                heartbeatTask,
                0,
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS);

        executorService.submit(() -> {
            try {
                awaitShutdown();
                heartbeatFuture.cancel(true);
            } catch (Exception e) {
                System.err.println("server shutdown interrupted.");
            }
        });
    }

    public void stop() {
        System.out.println("Stopping Worker Server...");

        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("ExecutorService shutdown interrupted: " + e.getMessage());
        }

        closeGRPCConnection();
        System.out.println("Worker server stopped.");
    }

    public void startWorkerPool(WorkerServer server, int numWorkers) {
        System.out.println("Starting worker pool with " + numWorkers + " workers...");
        for (int i = 0; i < numWorkers; i++) {
            executorService.submit(() -> worker());
        }
    }

    public void worker() {
        activeTasks.incrementAndGet();

        while (!cancelToken.get()) {
            try {
                TaskRequest task = taskQueue.take();
                executorService.submit(() -> updateTaskStatus(task, TaskStatus.STARTED));
                processTask(task);
                executorService.submit(() -> updateTaskStatus(task, TaskStatus.COMPLETED));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        activeTasks.decrementAndGet();
    }

    public void processTask(TaskRequest task) {
        System.out.println("Processing task :" + task);

        try {
            Thread.sleep(taskProcessTime.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Task processing interrupted: " + task);
            return;
        }
        System.out.println("Completed task: " + task);
    }

    public boolean sendHeartbeat() throws Exception {
        String workerAddress = System.getenv("WORKER_ADDRESS");
        if (workerAddress == null || workerAddress.isEmpty()) {
            workerAddress = listener.getInetAddress().getHostAddress() + ":"
                    + listener.getLocalPort();
        } else {
            workerAddress += serverPort;
        }

        HeartbeatRequest request = HeartbeatRequest.newBuilder().setWorkerId(id)
                .setAddress(workerAddress).build();
        try {
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub = coordinatorServiceClient;
            stub.sendHeartbeat(request);
            return true;

        } catch (Exception e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
            return false;
        }
    }

    public void startGRPCServer() throws Exception {

        if (serverPort == null || serverPort.isEmpty()) {
            listener = new ServerSocket(0);
            serverPort = (":" + listener.getLocalPort());
        } else {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress(Integer.parseInt(serverPort.replace(":", ""))));
        }

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
            shutdownLatch.countDown();
        }));
        shutdownLatch.await();
        stop();
    }

    public void closeGRPCConnection() {
        if (grpcConnection != null) {
            grpcConnection.shutdown();
            logger.info("gRPC server stopped.");
        }

        if (listener != null) {
            try {
                listener.close();
            } catch (Exception e) {
                logger.warning("Error while closing the listener: " + e.getMessage());
            }
        }

        if (grpcConnection != null) {
            grpcConnection.shutdown();
            logger.info("Closed client connection with coordinator.");
        }
    }

    public void updateTaskStatus(TaskRequest task, TaskStatus status) {
        UpdateTaskStatusRequest request = UpdateTaskStatusRequest.newBuilder().setTaskId(task.getTaskId())
                .setStatus(status).setStartedAt(System.currentTimeMillis() / 1000)
                .setCompletedAt(System.currentTimeMillis() / 1000).build();

        coordinatorServiceClient.updateTaskStatus(request);
    }

}
