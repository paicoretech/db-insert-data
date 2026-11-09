package org.paic.insertdata.component;

import com.paicbd.smsc.dto.UtilsRecords;

import java.util.List;

public class ObjectsCreator {
    private ObjectsCreator() {
    }

    public static UtilsRecords.Cdr getDefaultCdr() {
        return new UtilsRecords.Cdr(
                "1734454595605",
                "1734454593830",
                "1734454595603",
                "MESSAGE",
                "1734454582187-9026385306105",
                "mno-message-id",
                "HTTP",
                "3",
                "Origination Network Name",
                "SP",
                "SS7",
                "2",
                "Destination Network Name",
                "GW",
                "1",
                "",
                "SENT",
                "",
                "1773",
                "1775",
                "0",
                "60",
                "1322888089",
                "1",
                "1",
                "0987654321",
                "1",
                "1",
                "",
                "73",
                "0",
                "8",
                "22220",
                "200",
                "8",
                "55566768",
                "550e8400e29b41d4a716446655440000",
                "730169999999212",
                "55566768",
                "",
                "22220",
                "This is a long message, with this we can test the reduce process.",
                "0",
                "",
                "0",
                "",
                "",
                "",
                "",
                "18",
                "1734454582187-9026385306105",
                0,
                "",
                "",
                "",
                "",
                "",
                "",
                "71021"
                );
    }

    public static UtilsRecords.CdrUssi getDefaultUssiCdr() {
        return getDefaultUssiCdr("1734454582187-9026385306105");
    }

    public static UtilsRecords.CdrUssi getDefaultUssiCdr(String id) {
        return new UtilsRecords.CdrUssi(
                "1734454595605",
                "1734454593830",
                "1734454595603",
                id,
                "SS7",
                "1",
                "SIP",
                "2",
                "SENT",
                "",
                "1000",
                "700000000",
                "1",
                "5977778883455",
                "",
                "",
                "*123#",
                "5055499393",
                "1 ok, 2 continue 3 exit",
                "END",
                "1322888089",
                "8",
                "6",
                "8",
                "1734454582187-9026385306105",
                "1",
                ""
        );
    }

    public static UtilsRecords.Cdr createCdrWithMessageId(String messageId) {
        UtilsRecords.Cdr defaultCdr = getDefaultCdr();
        return new UtilsRecords.Cdr(
                defaultCdr.recordDate(),
                defaultCdr.submitDate(),
                defaultCdr.deliveryDate(),
                defaultCdr.messageType(),
                messageId,
                defaultCdr.mnoMessageId(),
                defaultCdr.originationProtocol(),
                defaultCdr.originationNetworkId(),
                defaultCdr.originationNetworkName(),
                defaultCdr.originationType(),
                defaultCdr.destinationProtocol(),
                defaultCdr.destinationNetworkId(),
                defaultCdr.destinationNetworkName(),
                defaultCdr.destinationType(),
                defaultCdr.routingId(),
                defaultCdr.status(),
                defaultCdr.statusCode(),
                defaultCdr.comment(),
                defaultCdr.dialogDuration(),
                defaultCdr.processingTime(),
                defaultCdr.dataCoding(),
                defaultCdr.validityPeriod(),
                defaultCdr.addrSrcDigits(),
                defaultCdr.addrSrcTon(),
                defaultCdr.addrSrcNpi(),
                defaultCdr.addrDstDigits(),
                defaultCdr.addrDstTon(),
                defaultCdr.addrDstNpi(),
                defaultCdr.remoteDialogId(),
                defaultCdr.localDialogId(),
                defaultCdr.localSpc(),
                defaultCdr.localSsn(),
                defaultCdr.localGlobalTitleDigits(),
                defaultCdr.remoteSpc(),
                defaultCdr.remoteSsn(),
                defaultCdr.remoteGlobalTitleDigits(),
                defaultCdr.correlationId(),
                defaultCdr.imsi(),
                defaultCdr.nnnDigits(),
                defaultCdr.originatorSccpAddress(),
                defaultCdr.mtServiceCenterAddress(),
                defaultCdr.message(),
                defaultCdr.esmClass(),
                defaultCdr.udhi(),
                defaultCdr.registeredDelivery(),
                defaultCdr.msgReferenceNumber(),
                defaultCdr.totalSegment(),
                defaultCdr.segmentSequence(),
                defaultCdr.retryNumber(),
                defaultCdr.profileId(),
                defaultCdr.parentId(),
                defaultCdr.broadcastId(),
                defaultCdr.localTranslationType(),
                defaultCdr.remoteTranslationType(),
                defaultCdr.messagePriority(),
                defaultCdr.optionalParameters(),
                defaultCdr.source(),
                defaultCdr.destination(),
                defaultCdr.mccMncDestination()
        );
    }

    public static List<UtilsRecords.Cdr> createLargeCdrBatch(int size) {
        return java.util.stream.IntStream.range(0, size)
                .mapToObj(i -> {
                    long timestamp = System.currentTimeMillis() + i;
                    long nano = System.nanoTime() + i;
                    return createCdrWithMessageId(timestamp + "-" + nano);
                })
                .toList();
    }

    public static String generateRealisticMessageId() {
        return System.currentTimeMillis() + "-" + System.nanoTime();
    }
}
