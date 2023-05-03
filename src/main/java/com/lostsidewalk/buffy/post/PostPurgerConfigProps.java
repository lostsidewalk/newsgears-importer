package com.lostsidewalk.buffy.post;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("post.purger")
public class PostPurgerConfigProps {

    int maxUnreadAge;

    int maxReadAge;

    int maxPostAge;

    public int getMaxUnreadAge() {
        return maxUnreadAge;
    }

    @SuppressWarnings("unused")
    public void setMaxUnreadAge(int maxUnreadAge) {
        this.maxUnreadAge = maxUnreadAge;
    }

    public int getMaxReadAge() {
        return maxReadAge;
    }

    public void setMaxReadAge(int maxReadAge) {
        this.maxReadAge = maxReadAge;
    }

    public int getMaxPostAge() {
        return maxPostAge;
    }

    public void setMaxPostAge(int maxPostAge) {
        this.maxPostAge = maxPostAge;
    }
}
