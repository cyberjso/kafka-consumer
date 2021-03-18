package io.joliveira.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


class Consumer implements Runnable {
    private static  final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static KafkaClientMetrics consumerMetrics;
    private final int id;
    private final List<String> topics;
    private final KafkaConsumer<String, String> consumer;
    private final MeterRegistry meterRegistry;

    public Consumer(int id, String groupId, List<String> topics, MeterRegistry meterRegistry) {
        this.id = id;
        this.topics = topics;
        this.meterRegistry =  meterRegistry;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(props);

        this.consumerMetrics = new KafkaClientMetrics(consumer);
        this.consumerMetrics.bindTo(meterRegistry);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("consumer: {}, topic: {}, partition: {}, message: {}",id, record.topic(),  record.partition(), record.value());
                    consumer.commitAsync();

                    meterRegistry.
                            counter("kafka.consumer.messages.consumed",
                                    Tags.of("topic", record.topic(), "partition", String.valueOf(record.topic())))
                            .increment();
                }
            }
        } catch (WakeupException e) {
            logger.info("Closing consumer, nothing to do about that ");

        } catch (Exception e) {
            logger.error("Some unrecoverable error that should be handled", e);
        } finally {
            consumer.close();
            consumerMetrics.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
