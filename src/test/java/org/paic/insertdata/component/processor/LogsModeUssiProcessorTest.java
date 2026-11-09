package org.paic.insertdata.component.processor;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.paic.insertdata.component.LogsComponent;
import org.paic.insertdata.component.ObjectsCreator;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class LogsModeUssiProcessorTest {

    @Test
    @DisplayName("Should send CDR batch to LogsComponent when processing raw JSON")
    void shouldSendCdrBatchToLogsComponent() {
        UtilsRecords.CdrUssi cdr = ObjectsCreator.getDefaultUssiCdr();
        String json = Converter.valueAsString(cdr);
        List<String> rawList = List.of(json);

        LogsComponent mockLogsComponent = mock(LogsComponent.class);
        LogsModeUssiProcessor processor = new LogsModeUssiProcessor(mockLogsComponent);

        AtomicReference<Flux<List<UtilsRecords.CdrUssi>>> logsFluxRef = new AtomicReference<>();
        doAnswer(inv -> {
            Flux<List<UtilsRecords.CdrUssi>> flux = inv.getArgument(0);
            logsFluxRef.set(flux);
            return null;
        }).when(mockLogsComponent).storeInUssiLogFile(any());

        processor.processCdr(rawList);

        verify(mockLogsComponent, timeout(2000)).storeInUssiLogFile(any());

        StepVerifier.create(logsFluxRef.get().next())
                .assertNext(list -> {
                    assert list != null;
                    assert list.size() == 1;
                    String firstId = list.stream()
                            .findFirst()
                            .map(UtilsRecords.CdrUssi::id)
                            .orElse(null);
                    org.junit.jupiter.api.Assertions.assertEquals(cdr.id(), firstId);
                })
                .verifyComplete();
    }
}