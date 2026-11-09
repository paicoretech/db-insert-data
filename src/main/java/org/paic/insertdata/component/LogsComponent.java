package org.paic.insertdata.component;

import com.paicbd.smsc.dto.UtilsRecords;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.paic.insertdata.util.AppProperties;
import org.paic.insertdata.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class LogsComponent {
    private static final Logger cdrLogger = LoggerFactory.getLogger("cdrLogger");
    private final AppProperties appProperties;

    public void storeInLogFile(Flux<List<UtilsRecords.Cdr>> cdrFlux) {
        cdrFlux
                .map(batch -> batch.stream()
                        .map(cdr -> CommonUtils.createStringCdr(cdr, appProperties.getSeparator(), appProperties.getCdrMessageLength()))
                        .collect(Collectors.joining(System.lineSeparator())))
                .doOnNext(cdrLogger::info)
                .subscribeOn(Schedulers.boundedElastic())
                .doOnComplete(() -> log.warn("CDR data wrote successfully into the file"))
                .subscribe();
    }

    public void storeInUssiLogFile(Flux<List<UtilsRecords.CdrUssi>> cdrFlux) {
        cdrFlux
                .map(batch -> batch.stream()
                        .map(cdr -> CommonUtils.createStringUssiCdr(cdr, appProperties.getSeparator(), appProperties.getUssiCdrMessageLength()))
                        .collect(Collectors.joining(System.lineSeparator())))
                .doOnNext(cdrLogger::info)
                .subscribeOn(Schedulers.boundedElastic())
                .doOnComplete(() -> log.warn("USSI CDR data wrote successfully into the file"))
                .subscribe();
    }
}
