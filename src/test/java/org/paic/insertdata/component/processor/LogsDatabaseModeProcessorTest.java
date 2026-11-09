package org.paic.insertdata.component.processor;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.paic.insertdata.component.DatabaseComponent;
import org.paic.insertdata.component.LogsComponent;
import org.paic.insertdata.component.ObjectsCreator;
import org.paic.insertdata.util.ProcessorType;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class LogsDatabaseModeProcessorTest {

    @Test
    @DisplayName("Should send CDR batch to both LogsComponent and DatabaseComponent when processing raw JSON")
    void shouldSendCdrBatchToLogsAndDatabaseComponents() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        String json = Converter.valueAsString(cdr);
        List<String> rawList = List.of(json);

        DatabaseComponent mockDbComponent = mock(DatabaseComponent.class);
        LogsComponent mockLogsComponent = mock(LogsComponent.class);

        AtomicReference<List<UtilsRecords.Cdr>> logsListRef = new AtomicReference<>();
        AtomicReference<List<UtilsRecords.Cdr>> dbListRef   = new AtomicReference<>();

        CountDownLatch logsLatch = new CountDownLatch(1);
        CountDownLatch dbLatch   = new CountDownLatch(1);

        doAnswer(inv -> {
            Flux<List<UtilsRecords.Cdr>> flux = inv.getArgument(0);
            flux.subscribe(list -> {
                logsListRef.set(list);
                logsLatch.countDown();
            });
            return null;
        }).when(mockLogsComponent).storeInLogFile(any());

        doAnswer(inv -> {
            Flux<List<UtilsRecords.Cdr>> flux = inv.getArgument(0);
            flux.subscribe(list -> {
                dbListRef.set(list);
                dbLatch.countDown();
            });
            return null;
        }).when(mockDbComponent).storeInDatabase(any(), any(ProcessorType.class));

        LogsDatabaseModeProcessor processor = new LogsDatabaseModeProcessor(mockDbComponent, mockLogsComponent);
        processor.processCdr(rawList);

        verify(mockLogsComponent, timeout(2000)).storeInLogFile(any());
        verify(mockDbComponent,   timeout(2000)).storeInDatabase(any(), any(ProcessorType.class));

        await().atMost(Duration.ofSeconds(2)).until(() -> logsLatch.getCount() == 0);
        await().atMost(Duration.ofSeconds(2)).until(() -> dbLatch.getCount() == 0);

        List<UtilsRecords.Cdr> logsList = logsListRef.get();
        List<UtilsRecords.Cdr> dbList   = dbListRef.get();

        assertEquals(1, logsList.size());
        assertEquals(1, dbList.size());

        String logsFirst = logsList.stream().findFirst().map(UtilsRecords.Cdr::messageId).orElse(null);
        String dbFirst   = dbList.stream().findFirst().map(UtilsRecords.Cdr::messageId).orElse(null);

        assertEquals(cdr.messageId(), logsFirst);
        assertEquals(cdr.messageId(), dbFirst);
    }
}