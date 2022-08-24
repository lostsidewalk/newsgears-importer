package com.lostsidewalk.buffy;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


@Configuration
public class ImportQueueConfig {

    private final Queue<StagingPost> articleQueue = new ConcurrentLinkedQueue<>();

    private final Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();

    @Bean
    public Queue<StagingPost> articleQueue() {
        return this.articleQueue;
    }

    @Bean
    public Queue<Throwable> errorQueue() {
        return this.errorQueue;
    }
}
