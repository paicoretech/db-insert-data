package org.paic.insertdata.config;

import com.paicbd.smsc.utils.Generated;
import org.paic.insertdata.util.ApplicationMode;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Generated
public class DatabaseCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String mode = context.getEnvironment().getProperty("application.mode");
        boolean databaseInitiated = ApplicationMode.DATABASE.getValue().equalsIgnoreCase(mode)
                || ApplicationMode.LOGS_DATABASE.getValue().equalsIgnoreCase(mode);

        if (!databaseInitiated) {
            mode = context.getEnvironment().getProperty("application.ussi.mode");
            databaseInitiated = ApplicationMode.DATABASE.getValue().equalsIgnoreCase(mode)
                    || ApplicationMode.LOGS_DATABASE.getValue().equalsIgnoreCase(mode);
        }

        return databaseInitiated;
    }
}
