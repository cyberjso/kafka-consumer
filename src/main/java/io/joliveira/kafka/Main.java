package io.joliveira.kafka;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import io.micrometer.statsd.StatsdMeterRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final int THREAD_POOL_SIZE = 3;

    public static void main(String[] args) {
        String groupId = "consumer-example-v2";
        List<String> topics = Arrays.asList("topic-1", "topic-2");

        StatsdConfig config = new StatsdConfig() {
            public String get(String k) {
                return null;
            }

            public StatsdFlavor flavor() {
                return StatsdFlavor.DATADOG;
            }
        };

        MeterRegistry meterRegistry = new StatsdMeterRegistry(config, Clock.SYSTEM);

        final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        final List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            Consumer consumer = new Consumer(i, groupId, topics, meterRegistry);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Consumer consumer : consumers)
                consumer.shutdown();

            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

}
