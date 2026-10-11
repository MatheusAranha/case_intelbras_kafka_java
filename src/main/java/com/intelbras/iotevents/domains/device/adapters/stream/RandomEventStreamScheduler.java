package com.intelbras.iotevents.domains.device.adapters.stream;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomDeviceEventFactory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.port.EventPublisherPort;
import com.intelbras.iotevents.domains.device.core.port.RandomEventStreamPort;
import com.intelbras.iotevents.domains.device.core.port.RandomStreamMetricsPort;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RandomEventStreamScheduler implements RandomEventStreamPort {

    static final int HISTORY_LIMIT = 50;
    static final long TICK_INTERVAL_MILLIS = 100;

    private final EventPublisherPort eventPublisherPort;
    private final RandomDeviceEventFactory factory;
    private final RandomStreamMetricsPort metricsPort;
    private final Random random = new Random();
    private final Object lock = new Object();

    final Deque<DeviceEvent> history = new ArrayDeque<>();

    private ScheduledExecutorService tickExecutor;
    private ExecutorService workerPool;
    private volatile RandomEventStreamConfig currentConfig;
    private volatile Instant startedAt;
    private volatile boolean running = false;
    private final AtomicLong requestedCount = new AtomicLong();
    private final AtomicLong publishedCount = new AtomicLong();
    private double tickRemainder = 0.0;

    public RandomEventStreamScheduler(EventPublisherPort eventPublisherPort,
                                       RandomDeviceEventFactory factory,
                                       RandomStreamMetricsPort metricsPort) {
        this.eventPublisherPort = eventPublisherPort;
        this.factory = factory;
        this.metricsPort = metricsPort;
    }

    @Override
    public RandomEventStreamStatus start(RandomEventStreamConfig config) {
        ExecutionLog log = ExecutionLog.start(RandomEventStreamScheduler.class, "start")
            .context("eventsPerMinute", config.eventsPerMinute())
            .context("dlqRate", config.dlqRate())
            .context("duplicateRate", config.duplicateRate())
            .context("categories", config.categories());
        try {
            synchronized (lock) {
                stopInternal();
                this.currentConfig = config;
                this.startedAt = Instant.now();
                this.running = true;
                this.requestedCount.set(0);
                this.publishedCount.set(0);
                this.tickRemainder = 0.0;
                this.history.clear();
                this.workerPool = Executors.newCachedThreadPool();
                this.tickExecutor = Executors.newSingleThreadScheduledExecutor();
                this.tickExecutor.scheduleAtFixedRate(this::tick, 0, TICK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            }
            log.step("Started random event stream");
            RandomEventStreamStatus status = status();
            log.info("Random event stream started");
            return status;
        } catch (Exception ex) {
            log.step("Unhandled exception while starting random event stream");
            log.error("Failed to start random event stream", ex);
            throw ex;
        }
    }

    @Override
    public RandomEventStreamStatus stop() {
        ExecutionLog log = ExecutionLog.start(RandomEventStreamScheduler.class, "stop");
        try {
            synchronized (lock) {
                stopInternal();
            }
            log.step("Stopped random event stream");
            RandomEventStreamStatus status = status();
            log.info("Random event stream stopped");
            return status;
        } catch (Exception ex) {
            log.step("Unhandled exception while stopping random event stream");
            log.error("Failed to stop random event stream", ex);
            throw ex;
        }
    }

    private void stopInternal() {
        if (tickExecutor != null) {
            tickExecutor.shutdownNow();
            tickExecutor = null;
        }
        if (workerPool != null) {
            workerPool.shutdown();
            workerPool = null;
        }
        running = false;
    }

    @Override
    public RandomEventStreamStatus status() {
        return new RandomEventStreamStatus(running, currentConfig, startedAt, requestedCount.get(), publishedCount.get());
    }

    int dueCountForTick(double eventsPerMinute) {
        synchronized (lock) {
            double duePerTick = eventsPerMinute / 600.0;
            tickRemainder += duePerTick;
            int due = (int) Math.floor(tickRemainder);
            tickRemainder -= due;
            return due;
        }
    }

    void tick() {
        RandomEventStreamConfig cfg = currentConfig;
        ExecutorService pool = workerPool;
        if (cfg == null || pool == null) {
            return;
        }
        int dueCount = dueCountForTick(cfg.eventsPerMinute());
        for (int i = 0; i < dueCount; i++) {
            requestedCount.incrementAndGet();
            metricsPort.incrementRequested();
            pool.submit(() -> dispatchOne(cfg));
        }
    }

    void dispatchOne(RandomEventStreamConfig cfg) {
        try {
            DeviceEvent event = decideEvent(cfg, random);
            eventPublisherPort.publish(event);
            publishedCount.incrementAndGet();
            metricsPort.incrementPublished();
        } catch (Exception publishFailure) {
            // A single slot's failure must never stop the stream — requested-vs-published
            // divergence in /status is the visible signal, not a crashed scheduler.
        }
    }

    DeviceEvent decideEvent(RandomEventStreamConfig cfg, Random rnd) {
        synchronized (lock) {
            if (!history.isEmpty() && rnd.nextDouble() < cfg.duplicateRate()) {
                List<DeviceEvent> snapshot = List.copyOf(history);
                return snapshot.get(rnd.nextInt(snapshot.size()));
            }
        }
        List<EventCategory> categories = cfg.categories();
        EventCategory category = categories.get(rnd.nextInt(categories.size()));
        boolean forceDlq = rnd.nextDouble() < cfg.dlqRate();
        DeviceEvent event = factory.generate(category, forceDlq);
        synchronized (lock) {
            history.addLast(event);
            while (history.size() > HISTORY_LIMIT) {
                history.removeFirst();
            }
        }
        return event;
    }
}
