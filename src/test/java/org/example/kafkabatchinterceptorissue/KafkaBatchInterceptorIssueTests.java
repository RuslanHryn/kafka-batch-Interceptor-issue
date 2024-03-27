package org.example.kafkabatchinterceptorissue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest
@EmbeddedKafka(topics = {"test-batch-topic", "test-record-topic"})
class KafkaBatchInterceptorIssueTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TestListener testListener;

    @Test
    void testBatchListener(CapturedOutput output) {
        kafkaTemplate.send("test-batch-topic", "test message");

        await().untilAsserted(() -> {
            assertThat(testListener.getMessages()).hasSize(4);
        });

        // initial message and 3 retries
        assertThat(StringUtils.countOccurrencesOf(output.getAll(), "Processing batch of messages")).isEqualTo(4);
        assertThat(StringUtils.countOccurrencesOf(output.getAll(), "Successfully processed batch of messages")).isEqualTo(1);
        assertThat(StringUtils.countOccurrencesOf(output.getAll(), "Failed to process batch of messages")).isEqualTo(3);
    }

    @Test
    void testRecordListener(CapturedOutput output) {
        kafkaTemplate.send("test-record-topic", "test message");

        await().untilAsserted(() -> {
            assertThat(testListener.getMessages()).hasSize(4);
        });

        // initial message and 3 retries
        assertThat(StringUtils.countOccurrencesOf(output.getAll(), "Processing message")).isEqualTo(4);
        assertThat(StringUtils.countOccurrencesOf(output.getAll(), "Successfully processed message")).isEqualTo(1);
        assertThat(StringUtils.countOccurrencesOf(output.getAll(), "Failed to process message")).isEqualTo(3);
    }

    @TestConfiguration
    @EnableKafka
    static class TestConfig {

        @Autowired
        private EmbeddedKafkaBroker embeddedKafkaBroker;

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> batchKafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0, 4)));
            factory.setBatchListener(true);
            factory.setBatchInterceptor(new BatchInterceptor<>() {
                @Override
                public ConsumerRecords<String, String> intercept(
                        ConsumerRecords<String, String> records,
                        Consumer<String, String> consumer) {
                    log.info("Processing batch of messages");
                    return records;
                }

                @Override
                public void success(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
                    log.info("Successfully processed batch of messages");
                }

                @Override
                public void failure(
                        ConsumerRecords<String, String> records,
                        Exception exception,
                        Consumer<String, String> consumer) {
                    log.info("Failed to process batch of messages");
                }


            });
            factory.setRecordInterceptor(new RecordInterceptor<>() {
                @Override
                public ConsumerRecord<String, String> intercept(
                        ConsumerRecord<String, String> record,
                        Consumer<String, String> consumer) {
                    log.info("Processing message");
                    return record;
                }

                @Override
                public void success(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
                    log.info("Successfully processed message");
                }

                @Override
                public void failure(
                        ConsumerRecord<String, String> record,
                        Exception exception,
                        Consumer<String, String> consumer) {
                    log.info("Failed to process message");
                }
            });
            return factory;
        }

        @Bean
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> recordKafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0, 4)));
            factory.setRecordInterceptor(new RecordInterceptor<>() {
                @Override
                public ConsumerRecord<String, String> intercept(
                        ConsumerRecord<String, String> record,
                        Consumer<String, String> consumer) {
                    log.info("Processing message");
                    return record;
                }

                @Override
                public void success(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
                    log.info("Successfully processed message");
                }

                @Override
                public void failure(
                        ConsumerRecord<String, String> record,
                        Exception exception,
                        Consumer<String, String> consumer) {
                    log.info("Failed to process message");
                }
            });
            return factory;
        }


        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfigs());
        }

        @Bean
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return props;
        }

        @Bean
        public TestListener testListener() {
            return new TestListener();
        }
    }

    static class TestListener {

        private final BlockingQueue<String> messages = new LinkedBlockingDeque<>();

        @KafkaListener(topics = "test-batch-topic", containerFactory = "batchKafkaListenerContainerFactory")
        public void listenBatch(List<String> message) {
            messages.addAll(message);
            if (messages.size() < 4) {
                throw new RuntimeException("Simulated error");
            }
        }

        @KafkaListener(topics = "test-record-topic", containerFactory = "recordKafkaListenerContainerFactory")
        public void listen(String message) {
            messages.add(message);
            if (messages.size() < 4) {
                throw new RuntimeException("Simulated error");
            }
        }

        public BlockingQueue<String> getMessages() {
            return messages;
        }
    }
}
