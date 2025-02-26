package com.dts.taskscheduler.pkg.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.sql.DataSource;

public class CoordinatorServerModel {
    public final String serverPort;
    // public final Map<Integer, WorkerInfo> workerPool;
    public final ReentrantReadWriteLock workerPoolLock;
    public final int maxHeartBeatMisses;
    public final long heartbeatInterval;
    public final DataSource dbPool;
    public final ExecutorService executorService;

    public CoordinatorServerModel(String serverPort, DataSource dbPool) {
        this.serverPort = serverPort;
        // this.workerPool = new ConcurrentHashMap<>();
        this.workerPoolLock = new ReentrantReadWriteLock();
        this.maxHeartBeatMisses = 5;
        this.heartbeatInterval = 5000;
        this.dbPool = dbPool;
        this.executorService = Executors.newCachedThreadPool();
    }

}
