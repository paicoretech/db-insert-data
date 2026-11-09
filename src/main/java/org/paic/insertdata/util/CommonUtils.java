package org.paic.insertdata.util;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Slf4j
public class CommonUtils {
    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    @Generated
    private CommonUtils() {
        throw new IllegalStateException("Utility Class");
    }

    public static String createStringCdr(UtilsRecords.Cdr cdr, String separator, int messageLength) {
        return String.join(separator,
                convertMillisecondsToDateTimeString(cdr.recordDate()),
                convertMillisecondsToDateTimeString(cdr.submitDate()),
                convertMillisecondsToDateTimeString(cdr.deliveryDate()),
                cdr.messageType(),
                cdr.messageId(),
                Optional.ofNullable(cdr.mnoMessageId()).orElse(""),
                cdr.originationProtocol(),
                cdr.originationNetworkId(),
                Optional.ofNullable(cdr.originationNetworkName()).orElse(""),
                cdr.originationType(),
                cdr.destinationProtocol(),
                cdr.destinationNetworkId(),
                Optional.ofNullable(cdr.destinationNetworkName()).orElse(""),
                cdr.destinationType(),
                cdr.routingId(),
                cdr.status(),
                cdr.statusCode(),
                cdr.comment(),
                cdr.dialogDuration(),
                cdr.processingTime(),
                cdr.dataCoding(),
                cdr.validityPeriod(),
                cdr.addrSrcDigits(),
                cdr.addrSrcTon(),
                cdr.addrSrcNpi(),
                cdr.addrDstDigits(),
                cdr.addrDstTon(),
                cdr.addrDstNpi(),
                cdr.remoteDialogId(),
                cdr.localDialogId(),
                cdr.localSpc(),
                cdr.localSsn(),
                cdr.localGlobalTitleDigits(),
                cdr.remoteSpc(),
                cdr.remoteSsn(),
                cdr.remoteGlobalTitleDigits(),
                cdr.correlationId(),
                cdr.imsi(),
                cdr.nnnDigits(),
                cdr.originatorSccpAddress(),
                cdr.mtServiceCenterAddress(),
                truncateMessage(cdr.message(), messageLength),
                cdr.esmClass(),
                cdr.udhi(),
                cdr.registeredDelivery(),
                cdr.msgReferenceNumber(),
                cdr.totalSegment(),
                cdr.segmentSequence(),
                cdr.retryNumber(),
                cdr.profileId(),
                cdr.parentId(),
                String.valueOf(cdr.broadcastId()),
                cdr.localTranslationType(),
                cdr.remoteTranslationType(),
                cdr.messagePriority(),
                Optional.ofNullable(cdr.optionalParameters()).orElse(""),
                cdr.source(),
                cdr.destination()
        );
    }

    public static String createStringUssiCdr(UtilsRecords.CdrUssi cdr, String separator, int messageLength) {
        return String.join(separator,
                convertMillisecondsToDateTimeString(cdr.recordDate()),
                convertMillisecondsToDateTimeString(cdr.submitDate()),
                convertMillisecondsToDateTimeString(cdr.deliveryDate()),
                cdr.id(),
                cdr.originProtocol(),
                cdr.originNetworkId(),
                cdr.destProtocol(),
                cdr.destNetworkId(),
                cdr.status(),
                cdr.comment(),
                cdr.dialogDuration(),
                cdr.processingTime(),
                cdr.dataCoding(),
                cdr.globalTitle(),
                cdr.globalTitleIndicator(),
                cdr.translationType(),
                cdr.shortCode(),
                cdr.msisdn(),
                truncateMessage(cdr.shortMessage(), messageLength),
                cdr.tcapMessageType(),
                cdr.invokeId(),
                cdr.ussdSsn(),
                cdr.hlrSsn(),
                cdr.mscSsn(),
                cdr.parentId(),
                cdr.events(),
                cdr.errorCode()
        );
    }

    /**
     * Converts an ISO_DATE formatted date string to a LocalDate object.
     *
     * @param milliseconds the date string in ISO_DATE format
     * @return the corresponding LocalDate object, or null if conversion fails
     */
    private static String convertMillisecondsToDateTimeString(String milliseconds) {
        try {
            long timestamp = Long.parseLong(milliseconds);
            Instant instant = Instant.ofEpochMilli(timestamp);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZONE_ID);
            return formatter.format(instant);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Flux<List<UtilsRecords.Cdr>> getCdrBatches(List<String> cdrListInRaw) {
        return Flux.fromIterable(cdrListInRaw)
                .map(cdrStr -> Converter.stringToObject(cdrStr, UtilsRecords.Cdr.class))
                .collectSortedList(
                        Comparator.comparing(
                                UtilsRecords.Cdr::recordDate,
                                Comparator.nullsLast(Comparator.naturalOrder())
                        )
                )
                .flux();
    }

    public static Flux<List<UtilsRecords.CdrUssi>> getUssiCdrBatches(List<String> cdrListInRaw) {
        return Flux.fromIterable(cdrListInRaw)
                .map(cdrStr -> Converter.stringToObject(cdrStr, UtilsRecords.CdrUssi.class))
                .collectSortedList(
                        Comparator.comparing(
                                UtilsRecords.CdrUssi::recordDate,
                                Comparator.nullsLast(Comparator.naturalOrder())
                        )
                )
                .flux();
    }

    /**
     * Truncates message text based on the configured length.
     *
     * @param message the original message text
     * @param messageLength the desired message length (-1 for full, 0 for empty, positive for specific length)
     * @return the truncated message text
     */
    public static String truncateMessage(String message, int messageLength) {
        if (message == null || message.isEmpty()) {
            return message;
        }

        if (messageLength == -1) {
            return message;
        }

        if (messageLength == 0) {
            return "";
        }

        return message.length() > messageLength ? message.substring(0, messageLength) : message;
    }
}