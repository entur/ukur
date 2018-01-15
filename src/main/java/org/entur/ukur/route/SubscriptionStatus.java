package org.entur.ukur.route;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.entur.ukur.json.CustomLocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.HashMap;

public class SubscriptionStatus {

    @JsonSerialize(using = CustomLocalDateTimeSerializer.class)
    private LocalDateTime lastProcessed;
    private HashMap<Class<?>, Long> processedCounter = new HashMap<>();

    public void processed(Class<?> processedClass) {
        Long count = processedCounter.get(processedClass);
        if (count == null) {
            count = 0L;
        }
        processedCounter.put(processedClass, count+1);
        lastProcessed = LocalDateTime.now();
    }

    public LocalDateTime getLastProcessed() {
        return lastProcessed;
    }

    public HashMap<Class<?>, Long> getProcessedCounter() {
        return processedCounter;
    }
}
