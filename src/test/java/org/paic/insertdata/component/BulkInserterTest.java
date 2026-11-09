package org.paic.insertdata.component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.paic.insertdata.util.AppProperties;
import org.paic.insertdata.util.ProcessorType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BulkInserterTest {
    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private AppProperties appProperties;

    private BulkInserter bulkInserter;

    @BeforeEach
    void setUp() {
        bulkInserter = new BulkInserter(jdbcTemplate, appProperties);
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Update batch when config is correctly then do it")
    void updateBatchWhenConfigIsCorrectlyThenDoIt() throws SQLException {
        List<UtilsRecords.Cdr> cdrList = Arrays.asList(ObjectsCreator.getDefaultCdr(), ObjectsCreator.getDefaultCdr());
        ArgumentCaptor<String> sqlQueryCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<UtilsRecords.Cdr>> listCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Integer> batchSizeCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<ParameterizedPreparedStatementSetter<UtilsRecords.Cdr>> setterCaptor =
                ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);

        bulkInserter.saveCdrBulk(cdrList, ProcessorType.MESSAGE_PROCESSOR);
        verify(jdbcTemplate)
                .batchUpdate(sqlQueryCaptor.capture(), listCaptor.capture(), batchSizeCaptor.capture(), setterCaptor.capture());

        var sqlQuery = sqlQueryCaptor.getValue();
        var list = listCaptor.getValue();
        var batchSize = batchSizeCaptor.getValue();
        var setter = setterCaptor.getValue();

        assertNotNull(sqlQuery);
        assertNotNull(list);
        assertNotNull(batchSize);
        assertNotNull(setter);

        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        setter.setValues(mockPreparedStatement, cdrList.getFirst());

        assertTrue(sqlQuery.contains("INSERT INTO cdr ("));
        assertEquals(2, batchSize);
        assertEquals(2, list.size());
        verify(mockPreparedStatement, atLeastOnce()).setString(anyInt(), anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Update USSI batch when config is correctly then do it")
    void updateUssiBatchWhenConfigIsCorrectlyThenDoIt() throws SQLException {
        List<UtilsRecords.CdrUssi> cdrList = Arrays.asList(ObjectsCreator.getDefaultUssiCdr(), ObjectsCreator.getDefaultUssiCdr());
        ArgumentCaptor<String> sqlQueryCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<UtilsRecords.CdrUssi>> listCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Integer> batchSizeCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<ParameterizedPreparedStatementSetter<UtilsRecords.CdrUssi>> setterCaptor =
                ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);

        bulkInserter.saveCdrBulk(cdrList, ProcessorType.USSI_PROCESSOR);
        verify(jdbcTemplate)
                .batchUpdate(sqlQueryCaptor.capture(), listCaptor.capture(), batchSizeCaptor.capture(), setterCaptor.capture());

        var sqlQuery = sqlQueryCaptor.getValue();
        var list = listCaptor.getValue();
        var batchSize = batchSizeCaptor.getValue();
        var setter = setterCaptor.getValue();

        assertNotNull(sqlQuery);
        assertNotNull(list);
        assertNotNull(batchSize);
        assertNotNull(setter);

        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        setter.setValues(mockPreparedStatement, cdrList.getFirst());

        assertTrue(sqlQuery.contains("INSERT INTO cdr_ussi ("));
        assertEquals(2, batchSize);
        assertEquals(2, list.size());
        verify(mockPreparedStatement, atLeastOnce()).setString(anyInt(), anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MESSAGE_PROCESSOR", "USSI_PROCESSOR"})
    @DisplayName("Should handle exception inside processRecordFields when object is not a Cdr or USSI Cdr")
    void processRecordFieldsShouldCatchExceptionForInvalidObject(String processorTypeStr) throws Exception {
        ProcessorType processorType = ProcessorType.valueOf(processorTypeStr);
        Field[] fields;

        if (ProcessorType.MESSAGE_PROCESSOR.equals(processorType)) {
            fields = UtilsRecords.Cdr.class.getDeclaredFields();
        } else {
            fields = UtilsRecords.CdrUssi.class.getDeclaredFields();
        }

        Object invalidObject = new Object();
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

        Method method = BulkInserter.class.getDeclaredMethod(
                "processRecordFields",
                PreparedStatement.class,
                Object.class,
                Field[].class
        );
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(bulkInserter, mockPreparedStatement, invalidObject, fields));
        verifyNoInteractions(mockPreparedStatement);
    }

    @Test
    @DisplayName("setNotNullValue should use default case for unsupported type")
    void setNotNullValueShouldUseDefaultCaseForUnsupportedType() throws Exception {
        class CustomType {
            @Override
            public String toString() {
                return "custom";
            }
        }

        PreparedStatement ps = mock(PreparedStatement.class);
        Object value = new CustomType();

        Method method = BulkInserter.class.getDeclaredMethod(
                "setNotNullValue", PreparedStatement.class, int.class, Object.class, Class.class
        );
        method.setAccessible(true);

        method.invoke(bulkInserter, ps, 1, value, CustomType.class);
        verify(ps).setObject(1, value);
    }

    @ParameterizedTest
    @ValueSource(strings = {"MESSAGE_PROCESSOR", "USSI_PROCESSOR"})
    @DisplayName("processMessage should use full message when messageLength is -1")
    void processMessageShouldUseFullMessageWhenMessageLengthIsMinusOne(String processorTypeStr) throws Exception {
        ProcessorType processorType = ProcessorType.valueOf(processorTypeStr);
        String methodStr = "processMessage";
        if (ProcessorType.MESSAGE_PROCESSOR.equals(processorType)) {
            when(appProperties.getCdrMessageLength()).thenReturn(-1);
        } else {
            methodStr = "processUssiMessage";
            when(appProperties.getUssiCdrMessageLength()).thenReturn(-1);
        }

        Method method = BulkInserter.class.getDeclaredMethod(
                methodStr, PreparedStatement.class, int.class, Object.class
        );
        method.setAccessible(true);

        PreparedStatement ps = mock(PreparedStatement.class);
        String originalMessage = "Hello, this is a long test message.";

        method.invoke(bulkInserter, ps, 1, originalMessage);
        verify(ps).setString(1, originalMessage);
    }

    @ParameterizedTest
    @ValueSource(strings = {"MESSAGE_PROCESSOR", "USSI_PROCESSOR"})
    @DisplayName("processMessage should truncate message when messageLength is 20")
    void processMessageShouldTruncateMessageWhenMessageLengthIs20(String processorTypeStr) throws Exception {
        ProcessorType processorType = ProcessorType.valueOf(processorTypeStr);
        String methodStr = "processMessage";
        if (ProcessorType.MESSAGE_PROCESSOR.equals(processorType)) {
            when(appProperties.getCdrMessageLength()).thenReturn(20);
        } else {
            methodStr = "processUssiMessage";
            when(appProperties.getUssiCdrMessageLength()).thenReturn(20);
        }

        Method method = BulkInserter.class.getDeclaredMethod(methodStr, PreparedStatement.class, int.class, Object.class);
        method.setAccessible(true);

        PreparedStatement ps = mock(PreparedStatement.class);
        String originalMessage = "Hello, this is a long test message.";

        method.invoke(bulkInserter, ps, 1, originalMessage);
        verify(ps).setString(1, "Hello, this is a lon");
    }

    @ParameterizedTest
    @ValueSource(strings = {"MESSAGE_PROCESSOR", "USSI_PROCESSOR"})
    @DisplayName("processMessage should remove message when messageLength is 0")
    void processMessageShouldRemoveMessageWhenMessageLengthIsZero(String processorTypeStr) throws Exception {
        ProcessorType processorType = ProcessorType.valueOf(processorTypeStr);
        String methodStr = "processMessage";
        if (ProcessorType.MESSAGE_PROCESSOR.equals(processorType)) {
            when(appProperties.getCdrMessageLength()).thenReturn(0);
        } else {
            methodStr = "processUssiMessage";
            when(appProperties.getUssiCdrMessageLength()).thenReturn(0);
        }

        Method method = BulkInserter.class.getDeclaredMethod(methodStr, PreparedStatement.class, int.class, Object.class);
        method.setAccessible(true);

        PreparedStatement ps = mock(PreparedStatement.class);
        String originalMessage = "Hello, this is a long test message.";

        method.invoke(bulkInserter, ps, 1, originalMessage);
        verify(ps).setString(1, "");
    }

    @Test
    @DisplayName("setNullValue should use VARCHAR and OTHER based on type")
    void setNullValueShouldHandleVarcharAndOtherTypes() throws Exception {
        PreparedStatement ps = mock(PreparedStatement.class);

        Method method = BulkInserter.class.getDeclaredMethod(
                "setNullValue", PreparedStatement.class, int.class, Class.class
        );
        method.setAccessible(true);

        method.invoke(bulkInserter, ps, 1, String.class);
        verify(ps).setNull(1, java.sql.Types.VARCHAR);

        method.invoke(bulkInserter, ps, 2, Double.class);
        verify(ps).setNull(2, java.sql.Types.OTHER);
    }

    @SuppressWarnings("unchecked")
    private String capturedSqlQuery(List<?> list, ProcessorType processorType) {
        ArgumentCaptor<String> sqlQueryCaptor = ArgumentCaptor.forClass(String.class);
        bulkInserter.saveCdrBulk(list, processorType);
        verify(jdbcTemplate).batchUpdate(sqlQueryCaptor.capture(), any(List.class), anyInt(), any(ParameterizedPreparedStatementSetter.class));
        return sqlQueryCaptor.getValue();
    }

    private static long countPlaceholders(String sqlQuery) {
        return sqlQuery.chars().filter(c -> c == '?').count();
    }

    private static long countColumns(String sqlQuery) {
        String columns = sqlQuery.substring(sqlQuery.indexOf('(') + 1, sqlQuery.indexOf(')'));
        return columns.split(",").length;
    }

    private void invokeSetChargingDate(PreparedStatement ps, UtilsRecords.Cdr cdr, int index) throws Exception {
        Method method = BulkInserter.class.getDeclaredMethod("setChargingDate", PreparedStatement.class, UtilsRecords.Cdr.class, int.class);
        method.setAccessible(true);
        method.invoke(bulkInserter, ps, cdr, index);
    }

    @Test
    @DisplayName("Cdr insert includes the charging date column with its placeholder")
    void cdrInsertIncludesTheChargingDateColumn() {
        String sqlQuery = capturedSqlQuery(List.of(ObjectsCreator.getDefaultCdr()), ProcessorType.MESSAGE_PROCESSOR);

        assertTrue(sqlQuery.contains(", charging_date) VALUES ("));
        assertEquals(countColumns(sqlQuery), countPlaceholders(sqlQuery));
    }

    @Test
    @DisplayName("Ussi cdr insert does not include the charging date column")
    void ussiCdrInsertDoesNotIncludeTheChargingDateColumn() {
        String sqlQuery = capturedSqlQuery(List.of(ObjectsCreator.getDefaultUssiCdr()), ProcessorType.USSI_PROCESSOR);

        assertFalse(sqlQuery.contains("charging_date"));
        assertEquals(countColumns(sqlQuery), countPlaceholders(sqlQuery));
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Cdr setter writes the charging date after the record fields")
    void cdrSetterWritesTheChargingDateAfterTheRecordFields() throws SQLException {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        ArgumentCaptor<ParameterizedPreparedStatementSetter<UtilsRecords.Cdr>> setterCaptor =
                ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);
        bulkInserter.saveCdrBulk(List.of(cdr), ProcessorType.MESSAGE_PROCESSOR);
        verify(jdbcTemplate).batchUpdate(anyString(), any(List.class), anyInt(), setterCaptor.capture());

        PreparedStatement ps = mock(PreparedStatement.class);
        setterCaptor.getValue().setValues(ps, cdr);

        int chargingDateIndex = (int) Arrays.stream(UtilsRecords.Cdr.class.getDeclaredFields())
                .filter(field -> field.getAnnotation(JsonProperty.class) != null)
                .count() + 1;
        OffsetDateTime expected = OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(cdr.submitDate())), ZoneOffset.UTC);
        verify(ps).setObject(chargingDateIndex, expected);
    }

    @Test
    @DisplayName("Charging date is the submit date in UTC")
    void chargingDateIsTheSubmitDateInUtc() throws Exception {
        UtilsRecords.Cdr cdr = mock(UtilsRecords.Cdr.class);
        when(cdr.submitDate()).thenReturn("1734454593830");
        PreparedStatement ps = mock(PreparedStatement.class);

        invokeSetChargingDate(ps, cdr, 5);

        verify(ps).setObject(5, OffsetDateTime.ofInstant(Instant.ofEpochMilli(1734454593830L), ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Charging date falls back to the record date when the submit date is empty")
    void chargingDateFallsBackToTheRecordDate() throws Exception {
        UtilsRecords.Cdr cdr = mock(UtilsRecords.Cdr.class);
        when(cdr.submitDate()).thenReturn("");
        when(cdr.recordDate()).thenReturn("1734454595605");
        PreparedStatement ps = mock(PreparedStatement.class);

        invokeSetChargingDate(ps, cdr, 5);

        verify(ps).setObject(5, OffsetDateTime.ofInstant(Instant.ofEpochMilli(1734454595605L), ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Charging date is null when no date can be parsed")
    void chargingDateIsNullWhenNoDateCanBeParsed() throws Exception {
        UtilsRecords.Cdr cdr = mock(UtilsRecords.Cdr.class);
        when(cdr.submitDate()).thenReturn("not-a-date");
        when(cdr.recordDate()).thenReturn(null);
        PreparedStatement ps = mock(PreparedStatement.class);

        invokeSetChargingDate(ps, cdr, 5);

        verify(ps).setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
        verify(ps, never()).setObject(anyInt(), any());
    }
}
