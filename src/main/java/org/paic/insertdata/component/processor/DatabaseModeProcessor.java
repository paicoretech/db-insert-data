package org.paic.insertdata.component.processor;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.paic.insertdata.component.DatabaseComponent;
import org.paic.insertdata.util.CommonUtils;
import org.paic.insertdata.util.ProcessorType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "application.mode", havingValue = "database")
public class DatabaseModeProcessor {
    private final DatabaseComponent databaseComponent;

    @KafkaListener(
            topics = KafkaTopicsConstants.CDR_TOPIC,
            groupId = KafkaConsumerConstants.DB_INSERT_DATA_CDR_GROUP_ID)
    public void processCdr(List<String> cdrListInRaw) {
        Flux<List<UtilsRecords.Cdr>> sortedFlux = CommonUtils.getCdrBatches(cdrListInRaw);
        databaseComponent.storeInDatabase(sortedFlux, ProcessorType.MESSAGE_PROCESSOR);
    }
}
