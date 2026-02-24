package dev.onemount.iceburg.config;

import dev.onemount.iceburg.service.MarquezLineageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
public class LineageSyncScheduler {

    private final MarquezLineageService marquezLineageService;

    public LineageSyncScheduler(MarquezLineageService marquezLineageService) {
        this.marquezLineageService = marquezLineageService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        log.info("Application started. Initial lineage sync will run in 30 seconds.");
    }

    @Scheduled(fixedDelayString = "${lineage.sync.interval:600000}", initialDelay = 30000)
    public void scheduledLineageSync() {
        log.info("Running scheduled lineage sync...");
        try {
            marquezLineageService.syncLineage();
        } catch (Exception e) {
            log.error("Scheduled lineage sync failed", e);
        }
    }
}

