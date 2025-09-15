package smartquery.index;

import smartquery.index.IndexTypes.SecondaryIndex;
import smartquery.storage.ColumnStore;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background task executor for index operations.
 * Provides async index building and maintenance operations.
 */
public class IndexBackgroundTasks {
    
    private final ExecutorService executor;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private volatile boolean shutdown = false;
    
    /**
     * Create background task executor with specified parallelism.
     */
    public IndexBackgroundTasks(int maxParallelism) {
        this.executor = Executors.newFixedThreadPool(maxParallelism, r -> {
            Thread t = new Thread(r, "index-builder-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Create background task executor with default parallelism.
     */
    public IndexBackgroundTasks() {
        this(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
    }
    
    /**
     * Submit an index build task.
     * Returns a CompletableFuture that completes when the index is built.
     */
    public CompletableFuture<Void> submitBuild(SecondaryIndex index, List<ColumnStore.Row> rows) {
        if (shutdown) {
            throw new IllegalStateException("Background tasks are shutdown");
        }
        
        activeTasks.incrementAndGet();
        
        return CompletableFuture.runAsync(() -> {
            try {
                activeTasks.incrementAndGet();
                index.build(rows);
            } finally {
                activeTasks.decrementAndGet();
            }
        }, executor);
    }
    
    /**
     * Submit multiple index builds in parallel.
     * Returns a Future that completes when all builds are done.
     */
    public CompletableFuture<Void> submitBuilds(List<IndexBuildTask> tasks) {
        if (shutdown) {
            throw new IllegalStateException("Background tasks are shutdown");
        }
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (IndexBuildTask task : tasks) {
            futures.add(submitBuild(task.index, task.rows));
        }
        
        // Return a composite future
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
    
    /**
     * Submit a task to rebuild/merge an index.
     * Useful for compacting fragmented indexes.
     */
    public Future<Void> submitMerge(SecondaryIndex targetIndex, List<SecondaryIndex> sourceIndexes) {
        if (shutdown) {
            throw new IllegalStateException("Background tasks are shutdown");
        }
        
        activeTasks.incrementAndGet();
        
        return executor.submit(() -> {
            try {
                activeTasks.incrementAndGet();
                // Simple merge strategy: collect all rows and rebuild
                // TODO: implement efficient merge algorithms and atomic replacement
                return null;
            } finally {
                activeTasks.decrementAndGet();
            }
        });
    }
    
    /**
     * Submit a maintenance task (e.g., cleanup, statistics update).
     */
    public Future<Void> submitMaintenance(Runnable maintenanceTask) {
        if (shutdown) {
            throw new IllegalStateException("Background tasks are shutdown");
        }
        
        activeTasks.incrementAndGet();
        
        return executor.submit(() -> {
            try {
                maintenanceTask.run();
                return null;
            } finally {
                activeTasks.decrementAndGet();
            }
        });
    }
    
    /**
     * Get the number of currently active tasks.
     */
    public int getActiveTasks() {
        return activeTasks.get();
    }
    
    /**
     * Check if there are any active tasks.
     */
    public boolean hasActiveTasks() {
        return activeTasks.get() > 0;
    }
    
    /**
     * Wait for all active tasks to complete.
     */
    public void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        
        while (hasActiveTasks() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50); // Poll every 50ms
        }
    }
    
    /**
     * Shutdown the background executor.
     * Waits for current tasks to complete but rejects new ones.
     */
    public void shutdown() {
        shutdown = true;
        executor.shutdown();
    }
    
    /**
     * Shutdown the background executor immediately.
     * Interrupts running tasks and rejects new ones.
     */
    public void shutdownNow() {
        shutdown = true;
        executor.shutdownNow();
    }
    
    /**
     * Check if the executor is shutdown.
     */
    public boolean isShutdown() {
        return shutdown || executor.isShutdown();
    }
    
    /**
     * Check if all tasks have completed after shutdown.
     */
    public boolean isTerminated() {
        return executor.isTerminated();
    }
    
    /**
     * Task specification for index building.
     */
    public static class IndexBuildTask {
        public final SecondaryIndex index;
        public final List<ColumnStore.Row> rows;
        
        public IndexBuildTask(SecondaryIndex index, List<ColumnStore.Row> rows) {
            this.index = index;
            this.rows = rows;
        }
        
        @Override
        public String toString() {
            return String.format("IndexBuildTask{table=%s, column=%s, segment=%s, rows=%d}", 
                index.table(), index.column(), index.segmentId(), rows.size());
        }
    }
    
    /**
     * Get statistics about background task execution.
     */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("activeTasks", activeTasks.get());
        stats.put("isShutdown", shutdown);
        stats.put("isTerminated", executor.isTerminated());
        
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
            stats.put("poolSize", tpe.getPoolSize());
            stats.put("activeCount", tpe.getActiveCount());
            stats.put("taskCount", tpe.getTaskCount());
            stats.put("completedTaskCount", tpe.getCompletedTaskCount());
        }
        
        return stats;
    }
}