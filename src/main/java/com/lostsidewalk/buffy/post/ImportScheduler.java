package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.query.QueryDefinition;
import com.lostsidewalk.buffy.query.QueryDefinitionDao;
import com.lostsidewalk.buffy.query.QueryMetrics;
import com.lostsidewalk.buffy.query.QueryMetricsDao;
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

import static com.lostsidewalk.buffy.post.ImportScheduler.ImportSchedule.scheduleNamed;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Comparator.comparing;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;

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
        final Function<LocalDateTime, Boolean> predicate;

        ImportSchedule(String name, int maxMisses, Function<LocalDateTime, Boolean> predicate) {
            this.name = name;
            this.maxMisses = maxMisses;
            this.predicate = predicate;
        }

        boolean matches(LocalDateTime localDateTime) {
            return this.predicate.apply(localDateTime);
        }

        ImportSchedule downgrade() {
            if (this == A) {
                return B;
            } else if (this == B) {
                return C;
            }

            return D;
        }

        static Optional<ImportSchedule> scheduleNamed(String name) {
            for (ImportSchedule i : values()) {
                if (StringUtils.equals(i.name, name)) {
                    return of(i);
                }
            }
            return empty();
        }
    }

    @Autowired
    QueryDefinitionDao queryDefinitionDao;

    @Autowired
    QueryMetricsDao queryMetricsDao;

    @PostConstruct
    public void postConstruct() {
        log.info("Scheduler constructed");
    }

    @SuppressWarnings("unused")
    public void update() {
        try {
            List<QueryDefinition> allActiveQueries = queryDefinitionDao.findAllActive();
            List<Object[]> updates = new ArrayList<>();
            for (QueryDefinition q : allActiveQueries) {
                ImportSchedule currentSchedule = ImportSchedule.scheduleNamed(q.getImportSchedule()).orElse(null);
                List<QueryMetrics> queryMetrics = queryMetricsDao.findByQueryId(q.getUsername(), q.getId());
                ImportSchedule newSchedule = reschedule(q.getId(), currentSchedule, queryMetrics);
                if (newSchedule != null) {
                    updates.add(new Object[] { newSchedule.name, q.getId() });
                }
            }
            if (isNotEmpty(updates)) {
                queryDefinitionDao.updateImportSchedules(updates);
            }
        } catch (Exception e) {
            log.error("Something horrible happened while during the import schedule update: {}", e.getMessage(), e);
        }
    }

    private ImportSchedule reschedule(Long queryId, ImportSchedule targetSchedule, List<QueryMetrics> queryMetrics) {
        // sort query metrics by most recent
        queryMetrics.sort(comparing(QueryMetrics::getImportTimestamp).reversed());
        int importMisses = 0;
        log.debug("Inspecting query metrics: queryId={}, qmCt={}", queryId, size(queryMetrics));
        for (QueryMetrics qm : queryMetrics) {
            if (qm.getImportCt() != null && qm.getErrorType() == null) {
                Integer persistCt = qm.getPersistCt();
                ImportSchedule schedule = scheduleNamed(qm.getImportSchedule()).orElse(null);
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
            // the query to the next lower schedule
            ImportSchedule newSchedule = targetSchedule.downgrade();
            log.info("Downgrading import schedule for queryId={}: oldSchedule={}, newSchedule={}, importMisses={} exceeds maxMisses={}",
                    queryId, targetSchedule, newSchedule, importMisses, targetSchedule.maxMisses);
            return newSchedule;
        } else {
            log.debug("Import miss count ({}) within max limit ({}) for this schedule ({})", importMisses, targetSchedule.maxMisses, targetSchedule.name);
        }

        return null;
    }
}