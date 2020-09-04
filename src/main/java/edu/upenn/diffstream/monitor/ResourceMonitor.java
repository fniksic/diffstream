package edu.upenn.diffstream.monitor;

import edu.upenn.diffstream.matcher.StreamEquivalenceMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceMonitor implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceMonitor.class);

    private static final long MEGABYTE = 1024 * 1024;
    private static final int GC_CYCLE_SECONDS = 120;

    private final Thread monitorThread = new Thread(this);
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final Set<StreamEquivalenceMatcher<?>> matchers = ConcurrentHashMap.newKeySet();
    private final String unmatchedItemsFile;
    private final String memoryFile;

    private volatile boolean isRunning = true;

    public ResourceMonitor(String unmatchedItemsFile, String memoryFile) {
        this.unmatchedItemsFile = unmatchedItemsFile;
        this.memoryFile = memoryFile;
    }

    public ResourceMonitor(StreamEquivalenceMatcher<? extends Serializable> matcher,
                           String unmatchedItemsFile,
                           String memoryFile) {
        this(unmatchedItemsFile, memoryFile);
        matchers.add(matcher);
    }

    @Override
    public void run() {
        try (PrintWriter itemsWriter = new PrintWriter(new FileWriter(unmatchedItemsFile), true);
             PrintWriter memoryWriter = new PrintWriter(new FileWriter(memoryFile), true)) {
            int second = 0;
            while (isRunning) {
                if (second == 0) {
                    memoryMXBean.gc();
                }
                matchers.forEach(matcher -> itemsWriter.println(matcher.getStats()));
                memoryWriter.println(getMemoryUsage());
                Thread.sleep(1000);
                second = (second + 1) % GC_CYCLE_SECONDS;
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("Resource monitor error", e);
        }
    }

    private String getMemoryUsage() {
        long heapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / MEGABYTE;
        long nonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / MEGABYTE;
        return "Heap memory: " + heapMemory + " MB Non-heap memory: " + nonHeapMemory + " MB";
    }

    public void start() {
        LOG.debug("Starting the resource monitor");
        monitorThread.start();
    }

    @Override
    public void close() {
        LOG.debug("Stopping the resource monitor");
        isRunning = false;
        try {
            monitorThread.join();
            matchers.clear();
            LOG.debug("Joined with resource monitor's thread");
        } catch (InterruptedException e) {
            LOG.warn("Resource monitor thread interrupted", e);
        }
    }

    public void observe(StreamEquivalenceMatcher<? extends Serializable> matcher) {
        LOG.debug("Observing matcher with id={}", matcher.getId());
        matchers.add(matcher);
    }

    public void unobserve(StreamEquivalenceMatcher<? extends Serializable> matcher) {
        LOG.debug("Unobserving matcher with id={}", matcher.getId());
        matchers.remove(matcher);
    }

}
