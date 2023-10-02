package com.lostsidewalk.buffy.post;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


/**
 * Configuration properties class for configuring post purging settings.
 * This class is used to specify age limits for purging unread, read, and all posts.
 */
@Configuration
@ConfigurationProperties("post.purger")
public class PostPurgerConfigProps {

    int maxUnreadAge;

    int maxReadAge;

    int maxPostAge;

    /**
     * Default constructor; initializes the object.
     */
    PostPurgerConfigProps() {
        super();
    }

    /**
     * Maximum age (in days) for purging unread posts.
     *
     * @return The maximum age for purging unread posts.
     */
    public int getMaxUnreadAge() {
        return maxUnreadAge;
    }

    /**
     * Set the maximum age (in days) for purging unread posts.
     *
     * @param maxUnreadAge The maximum age for purging unread posts.
     */
    @SuppressWarnings("unused")
    public void setMaxUnreadAge(int maxUnreadAge) {
        this.maxUnreadAge = maxUnreadAge;
    }

    /**
     * Maximum age (in days) for purging read posts.
     *
     * @return The maximum age for purging read posts.
     */
    public int getMaxReadAge() {
        return maxReadAge;
    }

    /**
     * Set the maximum age (in days) for purging read posts.
     *
     * @param maxReadAge The maximum age for purging read posts.
     */
    @SuppressWarnings("unused")
    public void setMaxReadAge(int maxReadAge) {
        this.maxReadAge = maxReadAge;
    }

    /**
     * Maximum age (in days) for purging all posts (both read and unread).
     *
     * @return The maximum age for purging all posts.
     */
    public int getMaxPostAge() {
        return maxPostAge;
    }

    /**
     * Set the maximum age (in days) for purging all posts (both read and unread).
     *
     * @param maxPostAge The maximum age for purging all posts.
     */
    @SuppressWarnings("unused")
    public void setMaxPostAge(int maxPostAge) {
        this.maxPostAge = maxPostAge;
    }
}
