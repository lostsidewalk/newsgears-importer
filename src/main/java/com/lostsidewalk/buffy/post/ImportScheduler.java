package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.subscription.SubscriptionDefinition;
import com.lostsidewalk.buffy.subscription.SubscriptionDefinitionDao;
import com.lostsidewalk.buffy.subscription.SubscriptionMetrics;
import com.lostsidewalk.buffy.subscription.SubscriptionMetricsDao;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.lostsidewalk.buffy.post.ImportScheduler.ImportSchedule.importScheduleNamed;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Comparator.comparing;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;

/**
 * Component responsible for updating import schedules of active subscriptions based on historical import metrics.
 * It evaluates the performance of subscriptions and adjusts their import schedules accordingly.
 */
@Slf4j
@Component
public class ImportScheduler {

    enum ImportSchedule {
        // the 'A' group runs on every engine cycle (hourly)
        A("A", 6, instant -> true),
        // the 'B' group runs every 6 hours (500, 1100, 1700, 2300)
        B("B", 2, instant -> (instant.getHour() + 1) % 6 == 0),
        // the 'C' group runs every 12 hours (1100, 2300)
        C("C", 1, instant -> (instant.getHour() + 1) % 12 == 0),
        //  the 'D' group runs at the top of every day (0000)
        D("D", MAX_VALUE, instant -> (instant.getHour() == 0));

        final String name;
        final int maxMisses;
        final Function<? super LocalDateTime, Boolean> predicate;

        ImportSchedule(String name, int maxMisses, Function<? super LocalDateTime, Boolean> predicate) {
            this.name = name;
            this.maxMisses = maxMisses;
            this.predicate = predicate;
        }

        boolean matches(LocalDateTime localDateTime) {
            return predicate.apply(localDateTime);
        }

        ImportSchedule downgrade() {
            if (this == A) {
                return B;
            } else if (this == B) {
                return C;
            }

            return D;
        }

        static Optional<ImportSchedule> importScheduleNamed(String name) {
            for (ImportSchedule importSchedule : values()) {
                if (StringUtils.equals(importSchedule.name, name)) {
                    return of(importSchedule);
                }
            }
            return empty();
        }

        @Override
        public final String toString() {
            return "ImportSchedule{" +
                    "name='" + name + '\'' +
                    ", maxMisses=" + maxMisses +
                    ", predicate=" + predicate +
                    '}';
        }
    }

    @Autowired
    SubscriptionDefinitionDao subscriptionDefinitionDao;

    @Autowired
    SubscriptionMetricsDao subscriptionMetricsDao;

    /**
     * Default constructor; initializes the object.
     */
    ImportScheduler() {
    }

    /**
     * Initializes the ImportScheduler after construction.
     * Logs an informational message to indicate the scheduler has been constructed.
     */
    @PostConstruct
    protected static void postConstruct() {
        log.info("Scheduler constructed");
    }

    /**
     * Updates the import schedules of active subscriptions based on historical import metrics.
     * Evaluates subscription performance and adjusts import schedules accordingly.
     */
    @SuppressWarnings("unused")
    public final void update() {
        try {
            List<SubscriptionDefinition> allActiveSubscriptions = subscriptionDefinitionDao.findAllActive();
            List<Object[]> updates = new ArrayList<>(size(allActiveSubscriptions));
            for (SubscriptionDefinition q : allActiveSubscriptions) {
                ImportSchedule currentSchedule = importScheduleNamed(q.getImportSchedule()).orElse(null);
                List<SubscriptionMetrics> subscriptionMetrics = subscriptionMetricsDao.findBySubscriptionId(q.getUsername(), q.getId());
                ImportSchedule newSchedule = reschedule(q.getId(), currentSchedule, subscriptionMetrics);
                if (newSchedule != null) {
                    updates.add(new Object[] { newSchedule.name, q.getId() });
                }
            }
            if (isNotEmpty(updates)) {
                subscriptionDefinitionDao.updateImportSchedules(updates);
            }
        } catch (DataAccessException | RuntimeException e) {
            log.error("Something horrible happened while during the import schedule update: {}", e.getMessage(), e);
        }
    }

    private static ImportSchedule reschedule(Long subscriptionId, ImportSchedule targetSchedule, List<? extends SubscriptionMetrics> subscriptionMetrics) {
        // sort subscription metrics by most recent
        subscriptionMetrics.sort(comparing(SubscriptionMetrics::getImportTimestamp).reversed());
        int importMisses = 0;
        log.debug("Inspecting subscription metrics: subscriptionId={}, qmCt={}", subscriptionId, size(subscriptionMetrics));
        for (SubscriptionMetrics qm : subscriptionMetrics) {
            if (qm.getImportCt() != null && qm.getErrorType() == null) {
                Integer persistCt = qm.getPersistCt();
                ImportSchedule schedule = importScheduleNamed(qm.getImportSchedule()).orElse(null);
                boolean scheduleMatches = schedule == targetSchedule;
                if (persistCt == 0 && scheduleMatches) {
                    importMisses++;
                } else {
                    break;
                }
            }
        }
        if (importMisses > targetSchedule.maxMisses) {
            // if the number of import misses for the current schedule is exceeded, downgrade
            // the subscription to the next lower schedule
            ImportSchedule newSchedule = targetSchedule.downgrade();
            log.debug("Downgrading import schedule for subscriptionId={}: oldSchedule={}, newSchedule={}, importMisses={} exceeds maxMisses={}",
                    subscriptionId, targetSchedule, newSchedule, importMisses, targetSchedule.maxMisses);
            return newSchedule;
        } else {
            log.trace("Import miss count ({}) within max limit ({}) for this schedule ({})", importMisses, targetSchedule.maxMisses, targetSchedule.name);
        }

        return null;
    }

    @Override
    public final String toString() {
        return "ImportScheduler{" +
                "subscriptionDefinitionDao=" + subscriptionDefinitionDao +
                ", subscriptionMetricsDao=" + subscriptionMetricsDao +
                '}';
    }
}
