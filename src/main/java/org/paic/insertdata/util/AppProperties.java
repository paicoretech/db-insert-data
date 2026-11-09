package org.paic.insertdata.util;

import com.paicbd.smsc.utils.Generated;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Generated
@Component
public class AppProperties {

    @Value("${configuration.cdr-workers}")
    private int cdrWorkers;

    @Value("${configuration.cdr-batch-size}")
    private int cdrBatchSize;

    @Value("${configuration.interval-millis}")
    private long intervalMillis;

    @Value("${application.mode:logs}")
    private String applicationMode;

    @Value("${application.ussi.mode:logs}")
    private String applicationUssiMode;

    @Value("${application.cdr.separator}")
    private String separator;

    @Value("${jdbc.max-retries:5}")
    private int maxRetries;

    @Value("${spring.datasource.url}")
    private String datasourceUrl;

    @Value("${spring.datasource.username}")
    private String datasourceUsername;

    @Value("${spring.datasource.password}")
    private String datasourcePassword;

    @Value("${spring.datasource.driver-class-name}")
    private String datasourceDriverClassName;

    @Value("${application.cdr.message-length}")
    private int cdrMessageLength;

    @Value("${application.ussi.cdr.message-length}")
    private int ussiCdrMessageLength;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.listener.concurrency}")
    private int kafkaListenerConcurrency;

    @Value("${spring.kafka.consumer.reconnect.backoff.ms:1000}")
    private long kafkaConsumerReconnectBackoffMs;

    @Value("${spring.kafka.consumer.reconnect.backoff.max.ms:10000}")
    private long kafkaConsumerReconnectBackoffMaxMs;

    @Value("${spring.kafka.consumer.session.timeout.ms:30000}")
    private int kafkaConsumerSessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat.interval.ms:10000}")
    private int kafkaConsumerHeartbeatIntervalMs;

    @Value("${spring.kafka.producer.reconnect.backoff.ms:1000}")
    private long kafkaProducerReconnectBackoffMs;

    @Value("${spring.kafka.producer.reconnect.backoff.max.ms:10000}")
    private long kafkaProducerReconnectBackoffMaxMs;
}