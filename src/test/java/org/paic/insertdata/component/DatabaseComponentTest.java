package org.paic.insertdata.component;

import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.paic.insertdata.util.AppProperties;
import org.paic.insertdata.util.ProcessorType;
import org.springframework.dao.DataAccessException;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class DatabaseComponentTest {

    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

    @Mock
    private AppProperties appProperties;

    @Mock
    private BulkInserter bulkInserter;

    @InjectMocks
    private DatabaseComponent databaseComponent;

    @Test
    @DisplayName("Store single CDR in database and verify complete database storage flow")
    void storeInDatabaseWhenSingleCdrThenStoreCompletelyAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        List<UtilsRecords.Cdr> cdrList = List.of(cdr);
        when(appProperties.getMaxRetries()).thenReturn(3);

        databaseComponent.storeInDatabase(Flux.just(cdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            verify(bulkInserter, times(1)).saveCdrBulk(cdrCaptor.capture(), eq(ProcessorType.MESSAGE_PROCESSOR));

            List<UtilsRecords.Cdr> capturedCdrs = cdrCaptor.getValue();
            assertEquals(1, capturedCdrs.size(), "Should store exactly one CDR");

            UtilsRecords.Cdr capturedCdr = capturedCdrs.getFirst();
            assertEquals(cdr.messageId(), capturedCdr.messageId(), "CDR messageId should match");
            assertEquals(cdr.addrSrcDigits(), capturedCdr.addrSrcDigits(), "CDR source address should match");
            assertEquals(cdr.addrDstDigits(), capturedCdr.addrDstDigits(), "CDR destination address should match");
            assertEquals(cdr.messageType(), capturedCdr.messageType(), "CDR message type should match");
            assertEquals(cdr.originationProtocol(), capturedCdr.originationProtocol(), "CDR origination protocol should match");
        });
    }

    @Test
    @DisplayName("Store multiple CDRs batch in database and verify batch storage flow")
    void storeInDatabaseWhenMultipleCdrsThenStoreAllAndVerifyBatchFlow() {
        UtilsRecords.Cdr cdr1 = ObjectsCreator.createCdrWithMessageId("1719421854360-11028072268466");
        UtilsRecords.Cdr cdr2 = ObjectsCreator.createCdrWithMessageId("1719421854361-11028072268467");
        UtilsRecords.Cdr cdr3 = ObjectsCreator.createCdrWithMessageId("1719421854362-11028072268468");
        List<UtilsRecords.Cdr> cdrList = List.of(cdr1, cdr2, cdr3);

        when(appProperties.getMaxRetries()).thenReturn(3);

        databaseComponent.storeInDatabase(Flux.just(cdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<ProcessorType> processorTypeCaptor = ArgumentCaptor.forClass(ProcessorType.class);
            verify(bulkInserter, times(1)).saveCdrBulk(cdrCaptor.capture(), processorTypeCaptor.capture());

            List<UtilsRecords.Cdr> capturedCdrs = cdrCaptor.getValue();
            assertEquals(3, capturedCdrs.size(), "Should store exactly three CDRs in batch");

            ProcessorType processorType = processorTypeCaptor.getValue();
            assertEquals(ProcessorType.MESSAGE_PROCESSOR, processorType);

            List<String> messageIds = capturedCdrs.stream()
                    .map(UtilsRecords.Cdr::messageId)
                    .toList();

            assertTrue(messageIds.contains("1719421854360-11028072268466"), "Batch should contain first CDR messageId");
            assertTrue(messageIds.contains("1719421854361-11028072268467"), "Batch should contain second CDR messageId");
            assertTrue(messageIds.contains("1719421854362-11028072268468"), "Batch should contain third CDR messageId");
        });
    }

    @Test
    @DisplayName("Store CDRs with database retry mechanism and verify retry flow")
    void storeInDatabaseWhenDatabaseFailsThenRetryAndVerifyRetryFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        List<UtilsRecords.Cdr> cdrList = List.of(cdr);
        when(appProperties.getMaxRetries()).thenReturn(3);

        doThrow(new DataAccessException("First DB error") {})
                .doThrow(new DataAccessException("Second DB error") {})
                .doNothing()
                .when(bulkInserter).saveCdrBulk(cdrList, ProcessorType.MESSAGE_PROCESSOR);

        databaseComponent.storeInDatabase(Flux.just(cdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> verify(bulkInserter, times(3)).saveCdrBulk(cdrList, ProcessorType.MESSAGE_PROCESSOR));
    }

    @Test
    @DisplayName("Store CDRs with maximum retries exceeded and verify retry exhaustion flow")
    void storeInDatabaseWhenMaxRetriesExceededThenStopRetryingAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        List<UtilsRecords.Cdr> cdrList = List.of(cdr);
        when(appProperties.getMaxRetries()).thenReturn(2);

        doThrow(new DataAccessException("Persistent DB error") {})
                .when(bulkInserter).saveCdrBulk(cdrList, ProcessorType.MESSAGE_PROCESSOR);

        databaseComponent.storeInDatabase(Flux.just(cdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> verify(bulkInserter, times(2)).saveCdrBulk(cdrList, ProcessorType.MESSAGE_PROCESSOR));
    }

    @Test
    @DisplayName("Store multiple CDR batches sequentially and verify sequential database storage flow")
    void storeInDatabaseWhenMultipleBatchesThenStoreSequentiallyAndVerifyFlow() {
        UtilsRecords.Cdr cdr1 = ObjectsCreator.createCdrWithMessageId("1719421854370-11028072268470");
        UtilsRecords.Cdr cdr2 = ObjectsCreator.createCdrWithMessageId("1719421854371-11028072268471");
        UtilsRecords.Cdr cdr3 = ObjectsCreator.createCdrWithMessageId("1719421854372-11028072268472");
        UtilsRecords.Cdr cdr4 = ObjectsCreator.createCdrWithMessageId("1719421854373-11028072268473");

        when(appProperties.getMaxRetries()).thenReturn(3);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(
                List.of(cdr1, cdr2),
                List.of(cdr3, cdr4)
        );
        databaseComponent.storeInDatabase(cdrFlux, ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            verify(bulkInserter, times(2)).saveCdrBulk(cdrCaptor.capture(), eq(ProcessorType.MESSAGE_PROCESSOR));

            List<List<UtilsRecords.Cdr>> allBatches = cdrCaptor.getAllValues();
            assertEquals(2, allBatches.size(), "Should process exactly 2 batches");

            List<String> allMessageIds = allBatches.stream()
                    .flatMap(List::stream)
                    .map(UtilsRecords.Cdr::messageId)
                    .toList();

            assertTrue(allMessageIds.contains("1719421854370-11028072268470"), "Should contain first batch first CDR");
            assertTrue(allMessageIds.contains("1719421854371-11028072268471"), "Should contain first batch second CDR");
            assertTrue(allMessageIds.contains("1719421854372-11028072268472"), "Should contain second batch first CDR");
            assertTrue(allMessageIds.contains("1719421854373-11028072268473"), "Should contain second batch second CDR");
        });
    }

    @Test
    @DisplayName("Store empty CDR batch in database and verify empty batch handling flow")
    void storeInDatabaseWhenEmptyBatchThenHandleGracefullyAndVerifyFlow() {
        List<UtilsRecords.Cdr> emptyCdrList = List.of();
        when(appProperties.getMaxRetries()).thenReturn(3);

        databaseComponent.storeInDatabase(Flux.just(emptyCdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            verify(bulkInserter, times(1)).saveCdrBulk(cdrCaptor.capture(), eq(ProcessorType.MESSAGE_PROCESSOR));

            List<UtilsRecords.Cdr> capturedCdrs = cdrCaptor.getValue();
            assertTrue(capturedCdrs.isEmpty(), "Empty batch should result in empty database call");
        });
    }

    @Test
    @DisplayName("Store CDRs with various retry scenarios and verify retry behavior flow")
    void storeInDatabaseWhenVariousRetryScenariosThenHandleCorrectlyAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        List<UtilsRecords.Cdr> cdrList = List.of(cdr);
        when(appProperties.getMaxRetries()).thenReturn(4);

        doThrow(new DataAccessException("Connection timeout") {})
                .doThrow(new DataAccessException("Lock timeout") {})
                .doThrow(new DataAccessException("Transaction rollback") {})
                .doNothing()
                .when(bulkInserter).saveCdrBulk(cdrList, ProcessorType.MESSAGE_PROCESSOR);

        databaseComponent.storeInDatabase(Flux.just(cdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            verify(bulkInserter, times(4)).saveCdrBulk(cdrCaptor.capture(), eq(ProcessorType.MESSAGE_PROCESSOR));

            List<List<UtilsRecords.Cdr>> allAttempts = cdrCaptor.getAllValues();
            assertEquals(4, allAttempts.size(), "Should make exactly 4 attempts");

            allAttempts.forEach(attempt -> {
                assertEquals(1, attempt.size(), "Each retry attempt should contain the CDR");
                assertEquals(cdr.messageId(), attempt.getFirst().messageId(), "CDR messageId should be consistent across retries");
            });
        });
    }

    @Test
    @DisplayName("Store large CDR batch in database and verify performance and completion flow")
    void storeInDatabaseWhenLargeBatchThenProcessEfficientlyAndVerifyFlow() {
        List<UtilsRecords.Cdr> largeBatch = ObjectsCreator.createLargeCdrBatch(50);
        when(appProperties.getMaxRetries()).thenReturn(3);

        databaseComponent.storeInDatabase(Flux.just(largeBatch), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            verify(bulkInserter, times(1)).saveCdrBulk(cdrCaptor.capture(), eq(ProcessorType.MESSAGE_PROCESSOR));

            List<UtilsRecords.Cdr> capturedCdrs = cdrCaptor.getValue();
            assertEquals(50, capturedCdrs.size(), "Large batch should contain exactly 50 CDRs");

            // Verify batch contains valid CDR data
            assertFalse(capturedCdrs.isEmpty(), "Large batch should not be empty");
            capturedCdrs.forEach(cdr -> {
                assertNotNull(cdr.messageId(), "Each CDR should have a messageId");
                assertTrue(cdr.messageId().contains("-"), "MessageId should have realistic timestamp-nano format");
            });
        });
    }

    @Test
    @DisplayName("Store CDRs with immediate success and verify single attempt flow")
    void storeInDatabaseWhenImmediateSuccessThenCompleteInSingleAttemptAndVerifyFlow() {
        UtilsRecords.Cdr cdr1 = ObjectsCreator.createCdrWithMessageId("1719421854380-11028072268480");
        UtilsRecords.Cdr cdr2 = ObjectsCreator.createCdrWithMessageId("1719421854381-11028072268481");
        List<UtilsRecords.Cdr> cdrList = List.of(cdr1, cdr2);

        when(appProperties.getMaxRetries()).thenReturn(5);

        databaseComponent.storeInDatabase(Flux.just(cdrList), ProcessorType.MESSAGE_PROCESSOR);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            ArgumentCaptor<List<UtilsRecords.Cdr>> cdrCaptor = ArgumentCaptor.forClass(List.class);
            verify(bulkInserter, times(1)).saveCdrBulk(cdrCaptor.capture(), eq(ProcessorType.MESSAGE_PROCESSOR));

            List<UtilsRecords.Cdr> capturedCdrs = cdrCaptor.getValue();
            assertEquals(2, capturedCdrs.size(), "Should store both CDRs in single attempt");

            List<String> messageIds = capturedCdrs.stream()
                    .map(UtilsRecords.Cdr::messageId)
                    .toList();

            assertTrue(messageIds.contains("1719421854380-11028072268480"), "Should contain first CDR messageId");
            assertTrue(messageIds.contains("1719421854381-11028072268481"), "Should contain second CDR messageId");
        });
    }

}
