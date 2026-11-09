package org.paic.insertdata.component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.paic.insertdata.config.DatabaseCondition;
import org.paic.insertdata.util.AppProperties;
import org.paic.insertdata.util.ProcessorType;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@Conditional(DatabaseCondition.class)
public class DatabaseComponent {
    private final AppProperties appProperties;
    private final BulkInserter bulkInserter;

    public <T> void storeInDatabase(Flux<List<T>> cdrFlux, ProcessorType processorType) {
        cdrFlux.publishOn(Schedulers.boundedElastic())
                .flatMap(list -> Mono.fromRunnable(() -> cdrPerformed(list, processorType))
                        .subscribeOn(Schedulers.boundedElastic()))
                .doOnComplete(() -> log.warn("CDR data inserted successfully into the database -> {}", processorType.name()))
                .subscribe();
    }

    private <T> void cdrPerformed(List<T> list, ProcessorType processorType) {
        int retries = 0;
        boolean success = false;
        while (retries < appProperties.getMaxRetries() && !success) {
            try {
                bulkInserter.saveCdrBulk(list, processorType);
                success = true;
            } catch (DataAccessException e) {
                log.error("Error while saving cdr to database, retry number: {}", retries, e);
                retries++;
            }
        }
    }
}
