/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */

package org.entur.ukur.service;

import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class PrometheusMetricsService extends PrometheusMeterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(PrometheusMetricsService.class);

    private final String METRICS_PREFIX = "app.ukur.";

    private final String DATA_INBOUND_COUNTER_NAME = METRICS_PREFIX + "data.inbound";
    private final String DATA_INBOUND_SUBSCRIBED_COUNTER_NAME = METRICS_PREFIX + "data.inbound.subscribed";
    private final String DATA_OUTBOUND_COUNTER_NAME = METRICS_PREFIX + "data.outbound";
    private final String SUBSCRIPTIONS_COUNTER_NAME = METRICS_PREFIX + "subscriptions.count";

    public PrometheusMetricsService() {
        super(PrometheusConfig.DEFAULT);
    }

    @PreDestroy
    public void shutdown() {
        this.close();
    }

    @Override
    public String scrape() {
        update();
        return super.scrape();
    }

    private void update() {

//        gauge(SUBSCRIPTIONS_COUNTER_NAME, dataStorageService.getNumberOfSubscriptions());
    }

    public void registerIncomingSubscribedData(String dataType, int count) {
        super.counter(DATA_INBOUND_SUBSCRIBED_COUNTER_NAME, "datatype", dataType).increment(count);
    }

    public void registerIncomingData(String dataType, int count) {
        super.counter(DATA_INBOUND_COUNTER_NAME, "datatype", dataType).increment(count);
    }

    public void registerOutboundData(String dataType, int count) {
        super.counter(DATA_OUTBOUND_COUNTER_NAME, "datatype", dataType).increment(count);
    }
}
