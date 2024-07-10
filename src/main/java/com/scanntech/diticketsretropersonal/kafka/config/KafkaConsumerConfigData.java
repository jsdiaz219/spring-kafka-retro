package com.scanntech.diticketsretropersonal.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerConfigData {
    private Integer concurrency;
    private Integer pollTimeout;
    private Boolean batchListener;
    private String maxPollRecords;

    public String getMaxPollRecords() {
        return maxPollRecords;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public Integer getPollTimeout() {
        return pollTimeout;
    }

    public Boolean getBatchListener() {
        return batchListener;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public void setPollTimeout(Integer pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public void setBatchListener(Boolean batchListener) {
        this.batchListener = batchListener;
    }

    public void setMaxPollRecords(String maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }
}
