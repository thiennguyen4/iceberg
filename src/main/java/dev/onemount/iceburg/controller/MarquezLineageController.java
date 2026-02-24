package dev.onemount.iceburg.controller;

import dev.onemount.iceburg.service.MarquezLineageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/lineage")
public class MarquezLineageController {

    private final MarquezLineageService marquezLineageService;

    public MarquezLineageController(MarquezLineageService marquezLineageService) {
        this.marquezLineageService = marquezLineageService;
    }

    @PostMapping("/sync")
    public ResponseEntity<Map<String, String>> syncLineage() {
        log.info("Manual lineage sync triggered");

        try {
            marquezLineageService.syncLineage();
            return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Lineage sync completed successfully"
            ));
        } catch (Exception e) {
            log.error("Error during lineage sync", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "error",
                "message", e.getMessage()
            ));
        }
    }

    @GetMapping("/marquez/jobs")
    public ResponseEntity<?> getMarquezJobs() {
        try {
            return ResponseEntity.ok(marquezLineageService.getMarquezJobs());
        } catch (Exception e) {
            log.error("Error fetching Marquez jobs", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", e.getMessage()
            ));
        }
    }
}

