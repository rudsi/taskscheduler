package com.dts.taskscheduler.pkg.worker;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.dts.taskscheduler.pkg.grpc.CoordinatorServiceGrpc;
import com.dts.taskscheduler.pkg.grpc.Api.TaskRequest;
import com.dts.taskscheduler.pkg.grpc.Api.TaskStatus;
import com.dts.taskscheduler.pkg.model.WorkerServer;

import io.grpc.ManagedChannelBuilder;

public class WorkerServiceImpl {
    private static final Logger logger = Logger.getLogger(WorkerServer.class.getName());
    private static final Duration DEFAULT_HEARTBEAT = Duration.ofSeconds(10);
    private static final int workerPoolSize = 5;
    private final Duration taskProcessTime = Duration.ofSeconds(5);

    public WorkerServer newServer(String port, String coordinator) {
        return new WorkerServer(
                generateUniqueId(),
                port,
                coordinator,
                DEFAULT_HEARTBEAT,
                new BlockingQueue<>(100),
                new ConcurrentHashMap<>(),
                new ReentrantLock(),
                Executors.newCachedThreadPool());

    }

    private static int generateUniqueId() {
        return Math.abs(UUID.randomUUID().hashCode());
    }

    public void start(WorkerServer server) throws Exception {
        startWorkerPool(workerPoolSize);

        try {
            periodicHeartbeat(server);
            startGRPCServer();
            awaitShutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            closeGRPCConnection();
        }
        awaitShutdown(server);
    }

    public boolean connectToCoordinator(WorkerServer server) throws Exception {
        System.out.println("Connecting to coordinator...");

        try {
            server.setCoordinatorChannel(
                    ManagedChannelBuilder.forTarget(server.getCoordinatorAddress()).usePlaintext().build());
            server.setCoordinatorServiceClient(CoordinatorServiceGrpc.newStub(server.getCoordinatorChannel()));
            logger.info("Connected to coordinator!");
            return true;
        } catch (Exception e) {
            logger.severe("Failed to connect to coordinator: " + e.getMessage());
            return false;
        }

    }

    public void periodicHeartbeat(WorkerServer server) {
        server.getActiveTasks().incrementAndGet();

        Runnable heartbeatTask = () -> {
            try {
                if (!sendHeartbeat(server)) {
                    System.out.println("Failed to send heartbeat");
                }
            } catch (Exception e) {
                System.err.println("Exception during heartbeat: " + e.getMessage());
            }
        };

        server.setHeartbeatFuture(server.getScheduler().scheduleAtFixedRate(
                heartbeatTask,
                0,
                server.getHeartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS));

        server.setHeartbeatFuture(server.getHeartbeatFuture());

        server.getExecutorService().submit(() -> {
            try {
                awaitShutdown(server);
                server.getHeartbeatFuture().cancel(true);
            } catch (InterruptedException ignored) {
            }
        });
    }

    public void stop(WorkerServer server) {
        System.out.println("Stopping Worker Server...");

        if (server.getHeartbeatFuture() != null) {
            server.getHeartbeatFuture().cancel(true);
        }

        server.getExecutorService().shutdown();
        try {
            if (!server.getExecutorService().awaitTermination(5, TimeUnit.SECONDS)) {
                server.getExecutorService().shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("ExecutorService shutdown interrupted: " + e.getMessage());
        }

        closeGRPCConnection(server);
        System.out.println("Worker server stopped.");
    }

    public void startWorkerPool(WorkerServer server, int numWorkers) {
        System.out.println("Starting worker pool with " + numWorkers + " workers...");
        ExecutorService executorService = server.getExecutorService();
        for (int i = 0; i < numWorkers; i++) {
            executorService.execute(worker(server));
        }
    }

    public void worker(WorkerServer server) {
        server.getActiveTasks().incrementAndGet();
        ExecutorService executorService = server.getExecutorService();

        while (!server.getCancelToken().get()) {
            try {
                TaskRequest task = server.getTaskQueue().take();
                executorService.submit(() -> updateTaskStatus(task, TaskStatus.STARTED));
                processTask(server, task);
                executorService.submit(() -> updateTaskStatus(task, TaskStatus.COMPLETED));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        server.getActiveTasks().decrementAndGet();
    }

    public void processTask(WorkerServer server, TaskRequest task) {
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

}
