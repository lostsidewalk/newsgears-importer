package com.lostsidewalk.buffy.post;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("post.purger")
public class PostPurgerConfigProps {

    int maxAge;

    public int getMaxAge() {
        return maxAge;
    }

    @SuppressWarnings("unused")
    public void setMaxAge(int maxAge) {
        this.maxAge = maxAge;
    }
}
