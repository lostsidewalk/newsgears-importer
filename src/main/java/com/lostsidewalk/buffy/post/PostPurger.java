package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class PostPurger {

    @Autowired
    StagingPostDao stagingPostDao;

    @Autowired
    PostPurgerConfigProps configProps;

    @PostConstruct
    public void postConstruct() {
        log.info("Purger constructed");
    }

    @SuppressWarnings("unused")
    public long doPurge() throws DataAccessException, DataUpdateException {
        log.debug("Purger invoked, params={}", this.configProps);
        Map<String, List<Long>> idleStagingPosts = stagingPostDao.findAllIdle(configProps.getMaxAge());
        int ct = 0;
        for (Map.Entry<String, List<Long>> e : idleStagingPosts.entrySet()) {
            ct += stagingPostDao.deleteByIds(e.getKey(), e.getValue());
        }
        return ct;
    }
}
