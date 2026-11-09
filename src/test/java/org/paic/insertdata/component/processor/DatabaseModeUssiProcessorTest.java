package org.paic.insertdata.component.processor;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.paic.insertdata.component.BulkInserter;
import org.paic.insertdata.component.DatabaseComponent;
import org.paic.insertdata.component.ObjectsCreator;
import org.paic.insertdata.util.AppProperties;
import org.paic.insertdata.util.ProcessorType;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DatabaseModeUssiProcessorTest {

    @Test
    @DisplayName("Should deserialize CDR JSON and delegate batch to BulkInserter via DatabaseComponent")
    void shouldDeserializeCdrJsonAndDelegateToBulkInserter() {
        UtilsRecords.CdrUssi cdr = ObjectsCreator.getDefaultUssiCdr();
        String json = Converter.valueAsString(cdr);
        List<String> rawList = List.of(json);

        AppProperties mockProps = mock(AppProperties.class);
        when(mockProps.getMaxRetries()).thenReturn(3);

        BulkInserter mockInserter = mock(BulkInserter.class);

        DatabaseComponent dbComponent = new DatabaseComponent(mockProps, mockInserter);
        DatabaseModeUssiProcessor processor = new DatabaseModeUssiProcessor(dbComponent);

        processor.processCdr(rawList);

        verify(mockInserter, timeout(2000).atLeastOnce()).saveCdrBulk(
                argThat(list -> {
                    assertNotNull(list, "Inserted list should not be null");
                    assertEquals(1, list.size(), "Expected exactly one CDR");
                    return list.stream()
                            .findFirst()
                            .map(c -> (UtilsRecords.CdrUssi) c)
                            .map(UtilsRecords.CdrUssi::id)
                            .map(id -> id.equals(cdr.id()))
                            .orElse(false);
                }), eq(ProcessorType.USSI_PROCESSOR)
        );
    }
}