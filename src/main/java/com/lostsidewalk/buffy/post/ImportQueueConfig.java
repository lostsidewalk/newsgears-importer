package com.lostsidewalk.buffy.post;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Configuration class for defining import queues used in the post import process.
 * This class configures two blocking queues: one for staging posts and another for errors.
 * These queues are used to store staging posts and errors during the import process.
 */
@Slf4j
@Configuration
public class ImportQueueConfig {

    private final BlockingQueue<StagingPost> articleQueue = new LinkedBlockingQueue<>();

    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    /**
     * Default constructor; initializes the object.
     */
    ImportQueueConfig() {
       super();
    }

    /**
     * Creates a bean for the article queue.
     *
     * @return The article queue bean.
     */
    @Bean
    public BlockingQueue<StagingPost> articleQueue() {
        return this.articleQueue;
    }

    /**
     * Creates a bean for the error queue.
     *
     * @return The error queue bean.
     */
    @Bean
    public BlockingQueue<Throwable> errorQueue() {
        return this.errorQueue;
    }
}
