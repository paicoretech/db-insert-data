package org.paic.insertdata.component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.paicbd.smsc.dto.UtilsRecords;
import lombok.extern.slf4j.Slf4j;
import org.paic.insertdata.config.DatabaseCondition;
import org.paic.insertdata.util.AppProperties;
import org.paic.insertdata.util.CommonUtils;
import org.paic.insertdata.util.ProcessorType;
import org.springframework.context.annotation.Conditional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The BulkInserter class is responsible for inserting data in bulk to the database using the provided JdbcTemplate.
 * It provides methods to save a bulk list of CDRs objects asynchronously.
 */
@Slf4j
@Repository
@Conditional(DatabaseCondition.class)
public class BulkInserter {
    private static final List<String> CDR_EXTRA_COLUMNS = List.of("charging_date");

    private final JdbcTemplate jdbcTemplate;
    private final Field[] cdrCachedFiles;
    private final StringBuilder cdrSqlQuery;
    private final Field[] cdrUssiCachedFiles;
    private final StringBuilder cdrUssiSqlQuery;
    private final AppProperties appProperties;

    public BulkInserter(JdbcTemplate jdbcTemplate, AppProperties appProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.appProperties = appProperties;
        this.cdrCachedFiles = getAnnotatedCdrsFields(ProcessorType.MESSAGE_PROCESSOR);
        this.cdrSqlQuery = this.loadSqlQuery(new StringBuilder("INSERT INTO cdr ("), cdrCachedFiles, CDR_EXTRA_COLUMNS);
        this.cdrUssiCachedFiles = getAnnotatedCdrsFields(ProcessorType.USSI_PROCESSOR);
        this.cdrUssiSqlQuery = this.loadSqlQuery(new StringBuilder("INSERT INTO cdr_ussi ("), cdrUssiCachedFiles, List.of());
    }

    /**
     * Saves a bulk list of Cdr records asynchronously.
     *
     * @param list the list of Cdr records to save
     */
    @Async
    @Transactional
    public <T> void saveCdrBulk(List<T> list, ProcessorType processorType) {
        if (ProcessorType.MESSAGE_PROCESSOR.equals(processorType)) {
            jdbcTemplate.batchUpdate(cdrSqlQuery.toString(), list, list.size(),
                    (ps, cdr) -> {
                        this.processRecordFields(ps, cdr, cdrCachedFiles);
                        this.setChargingDate(ps, (UtilsRecords.Cdr) cdr, cdrCachedFiles.length + 1);
                    }
            );
        } else {
            jdbcTemplate.batchUpdate(cdrUssiSqlQuery.toString(), list, list.size(),
                    (ps, cdr) -> this.processRecordFields(ps, cdr, cdrUssiCachedFiles)
            );
        }
    }

    /**
     * Appends the fields annotated with @JsonProperty to the given SQL query.
     *
     * @param sql               the SQL query to be modified
     * @param objectCachedFields the fields to check for JSON property annotation
     * @param extraColumns      the columns that are filled by this service and are not part of the record
     * @return the modified SQL query with the fields appended
     */
    private StringBuilder loadSqlQuery(StringBuilder sql, Field[] objectCachedFields, List<String> extraColumns) {
        for (Field field : objectCachedFields) {
            JsonProperty annotation = field.getAnnotation(JsonProperty.class);
            if (annotation != null) {
                sql.append(annotation.value()).append(", ");
            }
        }
        extraColumns.forEach(column -> sql.append(column).append(", "));
        sql.setLength(sql.length() - 2); // remove last comma
        sql.append(") VALUES ("); // add VALUES keyword
        sql.append("?, ".repeat(objectCachedFields.length + extraColumns.size())); // add ? for each field
        sql.deleteCharAt(sql.length() - 2); // remove last comma
        sql.append(")"); // add closing parenthesis

        return sql;
    }

    /**
     * Returns an array of fields in the given class that are annotated with @JsonProperty.
     *
     * @return an array of fields annotated with @JsonProperty in the given class
     */
    private Field[] getAnnotatedCdrsFields(ProcessorType processorType) {
        if (ProcessorType.MESSAGE_PROCESSOR.equals(processorType)) {
            return Arrays.stream(UtilsRecords.Cdr.class.getDeclaredFields())
                    .filter(field -> field.getAnnotation(JsonProperty.class) != null)
                    .toArray(Field[]::new);
        } else {
            return Arrays.stream(UtilsRecords.CdrUssi.class.getDeclaredFields())
                    .filter(field -> field.getAnnotation(JsonProperty.class) != null)
                    .toArray(Field[]::new);
        }
    }

    /**
     * Processes the record fields and sets their values into the given PreparedStatement.
     * Each field value is obtained from the given pdu object and set into the PreparedStatement using the setValueToPreparedStatement method.
     *
     * @param ps            the PreparedStatement to set the field values into
     * @param pdu           the object containing the field values
     * @param cachedFiles   the array of Field objects representing the record fields
     */
    private void processRecordFields(PreparedStatement ps, Object pdu, Field[] cachedFiles) {
        try {
            int index = 1;
            for (Field field : cachedFiles) {
                this.setValueToPreparedStatement(pdu, ps, field, index);
                index++;
            }
        } catch (Exception e) {
            log.error("Error while processing record fields", e);
        }
    }

    /**
     * Sets the value of a field to a PreparedStatement based on the provided object, field, and index.
     *
     * @param object The object from which to retrieve the field value
     * @param ps The PreparedStatement to set the field value on
     * @param field The Field object representing the field to set
     * @param index The index of the parameter in the prepared statement
     * @throws SQLException If an error occurs while setting the value on the PreparedStatement
     */
    private void setValueToPreparedStatement(Object object, PreparedStatement ps, Field field, int index) throws SQLException {
        try {
            boolean allowCdrType = object instanceof UtilsRecords.Cdr || object instanceof UtilsRecords.CdrUssi;
            if (!allowCdrType) {
                throw new IllegalArgumentException("The object must be an instance of Cdr");
            }

            Method getter = object.getClass().getMethod(field.getName());
            Object value = getter.invoke(object);
            log.debug("Setting value {} to PreparedStatement at index {}", value, index);

            switch (field.getName()) {
                case "message" -> processMessage(ps, index, value);
                case "recordDate","submitDate","deliveryDate" -> setNotNullValue(ps, index, value, LocalDateTime.class);
                case "dialogDuration","processingTime","remoteDialogId","localDialogId" -> setNullValueOrDefault(ps, index, value, Long.class);
                case "localSpc","localSsn","remoteSpc","remoteSsn","registeredDelivery","totalSegment","segmentSequence","retryNumber","routingId",
                     "dataCoding","addrSrcTon","addrSrcNpi","addrDstTon","addrDstNpi", "broadcastId", "ussdSsn", "hlrSsn",
                     "mscSsn", "errorCode" -> setNullValueOrDefault(ps, index, value, int.class);
                case "shortMessage" -> processUssiMessage(ps, index, value);
                default -> setNullValueOrDefault(ps, index, value, String.class);
            }
        } catch (IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            log.error("Error setting the value to PreparedStatement", e);
        }
    }

    private void setChargingDate(PreparedStatement ps, UtilsRecords.Cdr cdr, int index) {
        try {
            Long epoch = parseEpoch(cdr.submitDate());
            if (Objects.isNull(epoch)) {
                epoch = parseEpoch(cdr.recordDate());
            }
            if (Objects.isNull(epoch)) {
                ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
                return;
            }
            ps.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(epoch), ZoneOffset.UTC));
        } catch (SQLException e) {
            log.error("Error setting the charging date to PreparedStatement", e);
        }
    }

    private Long parseEpoch(String value) {
        if (Objects.isNull(value) || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void processMessage(PreparedStatement ps, int index, Object value) throws SQLException {
        String message = (String) value;
        message = CommonUtils.truncateMessage(message, appProperties.getCdrMessageLength());
        setNotNullValue(ps, index, message, String.class);
    }

    private void processUssiMessage(PreparedStatement ps, int index, Object value) throws SQLException {
        String message = (String) value;
        message = CommonUtils.truncateMessage(message, appProperties.getUssiCdrMessageLength());
        setNotNullValue(ps, index, message, String.class);
    }

    private void setNullValueOrDefault(PreparedStatement ps, int index, Object value, Class<?> type) throws SQLException {
        boolean isStringType = String.class.equals(type);
        boolean isValidValue = value != null && (isStringType || !value.toString().isEmpty());

        if (isValidValue) {
            setNotNullValue(ps, index, value, type);
        } else {
            if (isStringType) {
                ps.setString(index, "");
            } else {
                setNullValue(ps, index, type);
            }
        }
    }

    private void setNotNullValue(PreparedStatement ps, int index, Object value, Class<?> fieldType) throws SQLException {
        if (fieldType == Long.class || fieldType == long.class) {
            ps.setLong(index, Long.parseLong(String.valueOf(value)));
        } else if (fieldType == Integer.class || fieldType == int.class) {
            ps.setInt(index, Integer.parseInt(String.valueOf(value)));
        } else if (fieldType == String.class) {
            ps.setString(index, value != null ? (String) value : "");
        } else if (fieldType == LocalDateTime.class) {
            LocalDateTime time = Instant.ofEpochMilli(Long.parseLong((String) value)).atZone(ZoneId.systemDefault()).toLocalDateTime();
            ps.setObject(index, time);
        } else {
            ps.setObject(index, value);
        }
    }

    private void setNullValue(PreparedStatement ps, int index, Class<?> fieldType) throws SQLException {
        int sqlType;
        if (fieldType == Long.class || fieldType == long.class) {
            sqlType = Types.BIGINT;
        } else if (fieldType == Integer.class || fieldType == int.class) {
            sqlType = Types.INTEGER;
        } else if (fieldType == String.class) {
            sqlType = Types.VARCHAR;
        } else {
            sqlType = Types.OTHER;
        }
        ps.setNull(index, sqlType);
    }
}
