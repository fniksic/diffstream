package edu.upenn.diffstream.monitor;

import edu.upenn.diffstream.matcher.StreamEquivalenceMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceMonitor implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceMonitor.class);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSSSSS");

    private static final long MEGABYTE = 1024 * 1024;
    private static final int GC_CYCLE_SECONDS = 120;

    private final Thread monitorThread = new Thread(this);
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final Map<Long, MatcherResources> matchers = new ConcurrentHashMap<>();
    private final String unmatchedItemsFile;
    private final String memoryFile;

    private volatile boolean isRunning = true;

    public ResourceMonitor(String unmatchedItemsFile, String memoryFile) {
        this.unmatchedItemsFile = unmatchedItemsFile;
        this.memoryFile = memoryFile;
    }

    public ResourceMonitor(StreamEquivalenceMatcher<? extends Serializable> matcher,
                           String unmatchedItemsFile,
                           String memoryFile) throws FileNotFoundException {
        this(unmatchedItemsFile, memoryFile);
        observe(matcher);
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
                memoryWriter.println(getMemoryUsage());
                matchers.forEach((id, resources) -> {
                    itemsWriter.println(getTimestamp() + " " + resources.matcher.getStats());
                    try {
                        writeDurations(resources);
                    } catch (final IOException e) {
                        LOG.error("Couldn't write to file", e);
                    }
                });
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

    private static String getTimestamp() {
        final LocalDateTime localInstant = LocalDateTime.now();
        return localInstant.format(TIMESTAMP_FORMATTER);
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
            matchers.forEach((id, resources) -> {
                try {
                    finalizeMatcherStatistics(resources);
                } catch (final IOException e) {
                    LOG.error("Couldn't close file", e);
                }
            });
            matchers.clear();
            LOG.debug("Joined with resource monitor's thread");
        } catch (InterruptedException e) {
            LOG.warn("Resource monitor thread interrupted", e);
        }
    }

    public void observe(StreamEquivalenceMatcher<? extends Serializable> matcher) throws FileNotFoundException {
        LOG.debug("Observing matcher with id={}", matcher.getId());
        final FileOutputStream fileOutputStream = new FileOutputStream("durations-matcher-id-" + matcher.getId() + ".bin");
        final DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        final MatcherResources matcherResources = new MatcherResources(matcher, outputStream);
        matcher.startCollectingStatistics();
        matchers.put(matcher.getId(), matcherResources);
    }

    public void unobserve(StreamEquivalenceMatcher<? extends Serializable> matcher) throws IOException {
        LOG.debug("Unobserving matcher with id={}", matcher.getId());
        final MatcherResources matcherResources = matchers.get(matcher.getId());
        finalizeMatcherStatistics(matcherResources);
        matchers.remove(matcher.getId());
    }

    private void finalizeMatcherStatistics(final MatcherResources matcherResources) throws IOException {
        matcherResources.matcher.stopCollectingStatistics();
        writeDurations(matcherResources);
        matcherResources.outputStream.close();
    }

    private void writeDurations(final MatcherResources matcherResources) throws IOException {
        synchronized (matcherResources) {
            final StreamEquivalenceMatcher<?> matcher = matcherResources.matcher;
            final int processedItems = matcher.getProcessedItems();
            if (processedItems > matcherResources.totalWrittenDurations) {
                for (int i = matcherResources.totalWrittenDurations; i < processedItems; ++i) {
                    matcherResources.outputStream.writeLong(matcher.getDurations().remove());
                }
            }
            matcherResources.totalWrittenDurations = processedItems;
        }
    }

    private static class MatcherResources {

        private final StreamEquivalenceMatcher<?> matcher;
        private final DataOutputStream outputStream;
        private int totalWrittenDurations = 0;

        public MatcherResources(final StreamEquivalenceMatcher<?> matcher, final DataOutputStream outputStream) {
            this.matcher = matcher;
            this.outputStream = outputStream;
        }

    }

}
