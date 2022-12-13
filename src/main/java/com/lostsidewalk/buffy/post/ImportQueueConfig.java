package com.lostsidewalk.buffy.post;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


@Slf4j
@Configuration
public class ImportQueueConfig {

    private final BlockingQueue<StagingPost> articleQueue = new LinkedBlockingQueue<>();

    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    @Bean
    public BlockingQueue<StagingPost> articleQueue() {
        return this.articleQueue;
    }

    @Bean
    public BlockingQueue<Throwable> errorQueue() {
        return this.errorQueue;
    }
}
