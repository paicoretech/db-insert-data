package org.paic.insertdata.component;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.paic.insertdata.util.AppProperties;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogsComponentTest {

    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

    @Mock
    private AppProperties appProperties;

    private LogsComponent logsComponent;
    private Logger cdrLogger;
    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {
        cdrLogger = (Logger) LoggerFactory.getLogger("cdrLogger");
        listAppender = new ListAppender<>();
        listAppender.start();
        cdrLogger.addAppender(listAppender);

        logsComponent = new LogsComponent(appProperties);
    }

    @AfterEach
    void tearDown() {
        if (listAppender != null) {
            cdrLogger.detachAppender(listAppender);
        }
    }

    @Test
    @DisplayName("Store single CDR in log file with full message")
    void storeInLogFileWhenSingleCdrWithFullMessageThenLogCompletelyAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of(cdr));
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected CDR log entries but got none");

                    ILoggingEvent logEvent = listAppender.list.getFirst();
                    String loggedMessage = logEvent.getFormattedMessage();

                    assertTrue(loggedMessage.contains(cdr.messageId()), "Log should contain CDR messageId");
                    assertTrue(loggedMessage.contains(cdr.addrSrcDigits()), "Log should contain CDR addrSrcDigits");
                    assertTrue(loggedMessage.contains(cdr.addrDstDigits()), "Log should contain CDR addrDstDigits");
                    assertTrue(loggedMessage.contains("|"), "Log should contain separator character");
                    assertTrue(loggedMessage.contains("MESSAGE"), "Log should contain CDR messageType");
                    assertTrue(loggedMessage.contains("HTTP"), "Log should contain CDR originationProtocol");
                    assertTrue(loggedMessage.contains(cdr.message()), "Log should contain full message text");
                });
    }

    @Test
    @DisplayName("Store multiple CDRs batch in log file")
    void storeInLogFileWhenMultipleCdrsThenLogAllAndVerifyBatchProcessingFlow() {
        UtilsRecords.Cdr cdr1 = ObjectsCreator.createCdrWithMessageId("1719421854353-11028072268459");
        UtilsRecords.Cdr cdr2 = ObjectsCreator.createCdrWithMessageId("1719421854354-11028072268460");
        UtilsRecords.Cdr cdr3 = ObjectsCreator.createCdrWithMessageId("1719421854355-11028072268461");

        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of(cdr1, cdr2, cdr3));
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected batch CDR log entries but got none");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();

                    assertTrue(loggedMessage.contains("1719421854353-11028072268459"), "Batch log should contain first CDR messageId");
                    assertTrue(loggedMessage.contains("1719421854354-11028072268460"), "Batch log should contain second CDR messageId");
                    assertTrue(loggedMessage.contains("1719421854355-11028072268461"), "Batch log should contain third CDR messageId");

                    String[] logLines = loggedMessage.split(System.lineSeparator());
                    assertEquals(3, logLines.length, "Batch should contain exactly 3 CDR lines");
                });
    }

    @Test
    @DisplayName("Store CDRs with custom separator")
    void storeInLogFileWhenCustomSeparatorThenUseCorrectSeparatorAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        when(appProperties.getSeparator()).thenReturn(",");
        when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of(cdr));
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected CDR log with custom separator");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();

                    assertTrue(loggedMessage.contains(","), "Log should contain custom comma separator");
                    assertFalse(loggedMessage.contains("|"), "Log should not contain default pipe separator");
                    assertTrue(loggedMessage.contains(cdr.messageId()), "Log should contain CDR messageId with custom separator");
                });
    }

    @Test
    @DisplayName("Store CDRs with truncated message when length is 20")
    void storeInLogFileWhenMessageLengthIs20ThenTruncateMessageAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(20);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of(cdr));
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected CDR log with truncated message");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();

                    assertTrue(loggedMessage.contains(cdr.messageId()), "Log should contain CDR messageId");
                    assertTrue(loggedMessage.contains("|"), "Log should contain separator");
                    assertNotNull(loggedMessage, "Logged message should not be null");
                    assertFalse(loggedMessage.trim().isEmpty(), "Logged message should not be empty");
                });
    }

    @Test
    @DisplayName("Store CDRs with no message when length is 0")
    void storeInLogFileWhenMessageLengthIsZeroThenRemoveMessageAndVerifyFlow() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(0);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of(cdr));
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected CDR log with no message");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();

                    assertTrue(loggedMessage.contains(cdr.messageId()), "Log should contain CDR messageId");
                    assertTrue(loggedMessage.contains("|"), "Log should contain separator");
                    assertFalse(loggedMessage.contains(cdr.message()), "Log should not contain full message text");
                });
    }

    @Test
    @DisplayName("Store multiple CDR batches sequentially")
    void storeInLogFileWhenMultipleBatchesThenProcessSequentiallyAndVerifyFlow() {
        UtilsRecords.Cdr cdr1 = ObjectsCreator.createCdrWithMessageId("1719421854356-11028072268462");
        UtilsRecords.Cdr cdr2 = ObjectsCreator.createCdrWithMessageId("1719421854357-11028072268463");
        UtilsRecords.Cdr cdr3 = ObjectsCreator.createCdrWithMessageId("1719421854358-11028072268464");
        UtilsRecords.Cdr cdr4 = ObjectsCreator.createCdrWithMessageId("1719421854359-11028072268465");

        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(
                List.of(cdr1, cdr2),
                List.of(cdr3, cdr4)
        );
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertEquals(2, listAppender.list.size(), "Expected exactly 2 batch log entries");

                    String firstBatchLog = listAppender.list.getFirst().getFormattedMessage();
                    assertTrue(firstBatchLog.contains("1719421854356-11028072268462"), "First batch should contain first CDR messageId");
                    assertTrue(firstBatchLog.contains("1719421854357-11028072268463"), "First batch should contain second CDR messageId");
                    assertFalse(firstBatchLog.contains("1719421854358-11028072268464"), "First batch should not contain third CDR messageId");

                    String secondBatchLog = listAppender.list.get(1).getFormattedMessage();
                    assertTrue(secondBatchLog.contains("1719421854358-11028072268464"), "Second batch should contain third CDR messageId");
                    assertTrue(secondBatchLog.contains("1719421854359-11028072268465"), "Second batch should contain fourth CDR messageId");
                    assertFalse(secondBatchLog.contains("1719421854356-11028072268462"), "Second batch should not contain first CDR messageId");
                });
    }

    @Test
    @DisplayName("Store empty CDR batch")
    void storeInLogFileWhenEmptyBatchThenHandleGracefullyAndVerifyFlow() {
        lenient().when(appProperties.getSeparator()).thenReturn("|");
        lenient().when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of());
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Empty batch should still generate a log entry");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();
                    assertTrue(loggedMessage.trim().isEmpty(), "Empty batch should log empty content");
                });
    }

    @Test
    @DisplayName("Store CDRs with various field values")
    void storeInLogFileWhenVariousFieldValuesThenSerializeCorrectlyAndVerifyFlow() {
        UtilsRecords.Cdr cdrWithSpecialChars = createCdrWithSpecialContent();
        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(List.of(cdrWithSpecialChars));
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected CDR log with special content");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();

                    assertTrue(loggedMessage.contains(cdrWithSpecialChars.messageId()), "Log should contain special messageId");
                    assertTrue(loggedMessage.contains("|"), "Log should contain separator");
                    assertNotNull(loggedMessage, "Logged message should handle special characters");
                });
    }

    @Test
    @DisplayName("Store large CDR batch")
    void storeInLogFileWhenLargeBatchThenProcessEfficientlyAndVerifyFlow() {
        List<UtilsRecords.Cdr> largeBatch = ObjectsCreator.createLargeCdrBatch(100);
        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.Cdr>> cdrFlux = Flux.just(largeBatch);
        logsComponent.storeInLogFile(cdrFlux);

        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertFalse(listAppender.list.isEmpty(), "Expected large batch to be logged");

                    String loggedMessage = listAppender.list.getFirst().getFormattedMessage();
                    String[] logLines = loggedMessage.split(System.lineSeparator());

                    assertEquals(100, logLines.length, "Large batch should contain exactly 100 CDR lines");
                    assertTrue(loggedMessage.contains("|"), "Batch should contain separator characters");
                    assertFalse(loggedMessage.trim().isEmpty(), "Batch log should not be empty");
                });
    }

    @Test
    @DisplayName("Store multiple USSI CDRs batch in log file")
    void storeInLogFileWhenMultipleUSSICdrsThenLogAllAndVerifyBatchProcessingFlow() {
        UtilsRecords.CdrUssi cdr1 = ObjectsCreator.getDefaultUssiCdr("1719421854353-11028072268459");
        UtilsRecords.CdrUssi cdr2 = ObjectsCreator.getDefaultUssiCdr("1719421854354-11028072268460");
        UtilsRecords.CdrUssi cdr3 = ObjectsCreator.getDefaultUssiCdr("1719421854355-11028072268461");

        when(appProperties.getSeparator()).thenReturn("|");
        when(appProperties.getUssiCdrMessageLength()).thenReturn(-1);

        Flux<List<UtilsRecords.CdrUssi>> cdrFlux = Flux.just(List.of(cdr1, cdr2, cdr3));
        logsComponent.storeInUssiLogFile(cdrFlux);

        await().atMost(FIVE_SECONDS).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertFalse(listAppender.list.isEmpty(), "Expected batch CDR log entries but got none");

            String loggedMessage = listAppender.list.getFirst().getFormattedMessage();

            assertTrue(loggedMessage.contains("1719421854353-11028072268459"), "Batch log should contain first CDR messageId");
            assertTrue(loggedMessage.contains("1719421854354-11028072268460"), "Batch log should contain second CDR messageId");
            assertTrue(loggedMessage.contains("1719421854355-11028072268461"), "Batch log should contain third CDR messageId");

            String[] logLines = loggedMessage.split(System.lineSeparator());
            assertEquals(3, logLines.length, "Batch should contain exactly 3 CDR lines");
        });
    }

    private UtilsRecords.Cdr createCdrWithSpecialContent() {
        return ObjectsCreator.createCdrWithMessageId(ObjectsCreator.generateRealisticMessageId());
    }
}