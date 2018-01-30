/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *  https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */

package org.entur.ukur.route;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.entur.ukur.json.CustomLocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.HashMap;

/**
 * Some subscriptions statistics for a single node.
 */
public class SubscriptionStatus {

    @JsonSerialize(using = CustomLocalDateTimeSerializer.class)
    private LocalDateTime lastProcessed;
    private HashMap<Class<?>, Long> processedCounter = new HashMap<>();
    @JsonSerialize(using = CustomLocalDateTimeSerializer.class)
    private LocalDateTime lastHandled;
    private HashMap<Class<?>, Long> handledCounter = new HashMap<>();

    public void processed(Class<?> processed) {
        lastProcessed = increase(processed, this.processedCounter);
    }

    public void handled(Class<?> handled) {
        lastHandled = increase(handled, this.handledCounter);
    }

    public LocalDateTime getLastProcessed() {
        return lastProcessed;
    }

    public HashMap<Class<?>, Long> getProcessedCounter() {
        return processedCounter;
    }

    public LocalDateTime getLastHandled() {
        return lastHandled;
    }

    public HashMap<Class<?>, Long> getHandledCounter() {
        return handledCounter;
    }

    private LocalDateTime increase(Class<?> c, HashMap<Class<?>, Long> counterMap) {
        Long count = counterMap.get(c);
        if (count == null) {
            count = 0L;
        }
        counterMap.put(c, count+1);
        return LocalDateTime.now();
    }
}
