package dev.onemount.iceburg.controller;

import dev.onemount.iceburg.dto.response.DbtFlowResponse;
import dev.onemount.iceburg.service.DbtService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/dbt")
@RequiredArgsConstructor
public class DbtController {

    private final DbtService dbtService;

    @PostMapping("/run")
    public ResponseEntity<DbtFlowResponse> triggerDbtFlow() {
        try {
            return ResponseEntity.ok(dbtService.triggerDbtFlow());
        } catch (Exception e) {
            log.error("Error triggering dbt flow", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(DbtFlowResponse.builder()
                            .success(false)
                            .message("Failed to trigger dbt flow: " + e.getMessage())
                            .build());
        }
    }

    @GetMapping("/status/{dagRunId}")
    public ResponseEntity<Map<String, Object>> getDagRunStatus(@PathVariable String dagRunId) {
        try {
            return ResponseEntity.ok(dbtService.getDagRunStatus(dagRunId));
        } catch (Exception e) {
            log.error("Error getting dag run status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

