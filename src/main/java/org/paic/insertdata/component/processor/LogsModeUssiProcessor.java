package org.paic.insertdata.component.processor;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.paic.insertdata.component.LogsComponent;
import org.paic.insertdata.util.CommonUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "application.ussi.mode", havingValue = "logs")
public class LogsModeUssiProcessor {
    private final LogsComponent logsComponent;

    @KafkaListener(
            topics = KafkaTopicsConstants.CDR_USSI_TOPIC,
            groupId = KafkaConsumerConstants.DB_INSERT_DATA_CDR_GROUP_ID)
    public void processCdr(List<String> cdrListInRaw) {

        Flux<List<UtilsRecords.CdrUssi>> sortedFlux = CommonUtils.getUssiCdrBatches(cdrListInRaw);
        logsComponent.storeInUssiLogFile(sortedFlux);
    }
}
