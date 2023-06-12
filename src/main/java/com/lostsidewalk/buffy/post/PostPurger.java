package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.queue.QueueDefinitionDao;
import com.lostsidewalk.buffy.subscription.SubscriptionMetricsDao;
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
    SubscriptionMetricsDao subscriptionMetricsDao;

    @Autowired
    QueueDefinitionDao queueDefinitionDao;

    @Autowired
    PostPurgerConfigProps configProps;

    @PostConstruct
    public void postConstruct() {
        log.info("Purger constructed");
    }

    @SuppressWarnings("unused")
    public int purgeArchivedPosts() throws DataAccessException, DataUpdateException {
        log.debug("Purging ARCHIVED staging posts, params={}", this.configProps);
        return stagingPostDao.purgeArchivedPosts(this.configProps.getMaxPostAge());
    }

    /**
     * posts that are READ and were imported GT max read age days ago -> ARCHIVED
     * posts that are UNREAD and were imported GT max unread age days ago -> ARCHIVED
     *
     * @return
     * @throws DataAccessException
     * @throws DataUpdateException
     */
    @SuppressWarnings("unused")
    public long markIdlePostsForArchive() throws DataAccessException, DataUpdateException {
        log.debug("Marking idle posts for archival, params={}", this.configProps);
        Map<String, List<Long>> idleStagingPosts = stagingPostDao.findAllIdle(configProps.getMaxUnreadAge(), configProps.getMaxReadAge());
        int ct = 0;
        for (Map.Entry<String, List<Long>> e : idleStagingPosts.entrySet()) {
            ct += stagingPostDao.archiveByIds(e.getKey(), e.getValue());
        }
        return ct;
    }

    @SuppressWarnings("unused")
    public long purgeDeletedQueues() throws DataAccessException, DataUpdateException {
        log.debug("Purging DELETED queues, params={}", this.configProps);
        return queueDefinitionDao.purgeDeleted();
    }

    @SuppressWarnings("unused")
    public long purgeOrphanedQueryMetrics() throws DataAccessException, DataUpdateException {
        log.debug("Purging ORPHANED query metrics, params={}", this.configProps);
        return subscriptionMetricsDao.purgeOrphaned();
    }
}
