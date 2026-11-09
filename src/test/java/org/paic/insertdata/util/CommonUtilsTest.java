package org.paic.insertdata.util;

import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.paic.insertdata.component.ObjectsCreator;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class CommonUtilsTest {

    @Test
    @DisplayName("Should create string cdr with full message when messageLength is -1")
    void createStringCdrWhenMessageLengthIsMinusOneThenUseFullMessage() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        String result = CommonUtils.createStringCdr(cdr, "|", -1);
        String[] fields = result.split("\\|", -1);
        assertEquals("This is a long message, with this we can test the reduce process.", fields[41]);

        UtilsRecords.CdrUssi ussiCdr = ObjectsCreator.getDefaultUssiCdr();
        String ussiResult = CommonUtils.createStringUssiCdr(ussiCdr, "|", -1);
        assertTrue(ussiResult.contains("1734454582187-9026385306105|SS7|1|SIP|2|SENT||1000|700000000|1|5977778883455|||*123#|5055499393|1 ok, 2 continue 3 exit|END|1322888089|8|6|8|1734454582187-9026385306105|1|"));
    }

    @Test
    @DisplayName("Should create string cdr with truncated message when messageLength is 20")
    void createStringCdrWhenMessageLengthIs20ThenTruncateMessage() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        String result = CommonUtils.createStringCdr(cdr, "|", 20);
        String[] fields = result.split("\\|", -1);
        assertEquals("This is a long messa", fields[41]);

        UtilsRecords.CdrUssi ussiCdr = ObjectsCreator.getDefaultUssiCdr();
        String ussiResult = CommonUtils.createStringUssiCdr(ussiCdr, "|", 20);
        assertTrue(ussiResult.contains("1734454582187-9026385306105|SS7|1|SIP|2|SENT||1000|700000000|1|5977778883455|||*123#|5055499393|1 ok, 2 continue 3 e|END|1322888089|8|6|8|1734454582187-9026385306105|1|"));
    }

    @Test
    @DisplayName("Should create string cdr with no message when messageLength is 0")
    void createStringCdrWhenMessageLengthIsZeroThenRemoveMessage() {
        UtilsRecords.Cdr cdr = ObjectsCreator.getDefaultCdr();
        String result = CommonUtils.createStringCdr(cdr, "|", 0);
        String[] fields = result.split("\\|", -1);
        String messageField = fields[41];
        assertEquals("", messageField);

        UtilsRecords.CdrUssi ussiCdr = ObjectsCreator.getDefaultUssiCdr();
        String ussiResult = CommonUtils.createStringUssiCdr(ussiCdr, "|", 0);
        assertTrue(ussiResult.contains("1734454582187-9026385306105|SS7|1|SIP|2|SENT||1000|700000000|1|5977778883455|||*123#|5055499393||END|1322888089|8|6|8|1734454582187-9026385306105|1|"));
        String[] ussiFields = ussiResult.split("\\|");
        String ussiMessageField = ussiFields[18];
        assertEquals("", ussiMessageField);
    }

    @Test
    @DisplayName("Should return null for invalid milliseconds string using reflection")
    void convertMillisecondsToDateTimeStringInvalid() throws Exception {
        Method method = CommonUtils.class.getDeclaredMethod("convertMillisecondsToDateTimeString", String.class);
        method.setAccessible(true);

        String input = "invalid";
        String result = (String) method.invoke(null, input);

        assertNull(result);
    }

    @ParameterizedTest
    @DisplayName("Should truncate message based on specified length")
    @CsvSource({
            "'Short message',-1,Short message",
            "'Short message',0,''",
            "'Short message',5,Short",
            "'Exactly 20 chars!!',20,Exactly 20 chars!!",
            "'This is a very long message that should be truncated',10,'This is a '",
            "'This is a very long message that should be truncated',100,'This is a very long message that should be truncated'"
    })
    void shouldTruncateMessageBasedOnLength(String input, int length, String expected) {
        String result = CommonUtils.truncateMessage(input, length);
        assertEquals(expected, result);
    }

    @Test
    @DisplayName("Should handle null and empty messages in truncateMessage")
    void truncateMessageShouldHandleNullAndEmpty() {
        assertNull(CommonUtils.truncateMessage(null, -1));
        assertEquals("", CommonUtils.truncateMessage("", 10));
        assertEquals("", CommonUtils.truncateMessage("", 0));
    }

    @ParameterizedTest
    @DisplayName("Should truncate message to 20 characters when length is 20")
    @CsvSource({
            "'Short message',Short message",
            "'Exactly 20 chars!!',Exactly 20 chars!!",
            "'This is a very long message that should be reduced','This is a very long '"
    })
    void shouldTruncateMessageTo20CharactersWhenLengthIs20(String input, String expected) {
        String result = CommonUtils.truncateMessage(input, 20);
        assertEquals(expected, result);
    }
}