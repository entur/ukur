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

import com.google.common.collect.Maps;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.entur.ukur.subscription.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Map;

@Component
public class PrometheusMetricsService extends PrometheusMeterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(PrometheusMetricsService.class);

    @Autowired
    private DataStorageService dataStorageService;

    private final String METRICS_PREFIX = "app.ukur.";

    private final String DATA_INBOUND_COUNTER_NAME = METRICS_PREFIX + "data.inbound";
    private final String DATA_INBOUND_SUBSCRIBED_COUNTER_NAME = METRICS_PREFIX + "data.inbound.subscribed";
    private final String DATA_OUTBOUND_COUNTER_NAME = METRICS_PREFIX + "data.outbound";
    private final String DATA_OUTBOUND_SUBSCRIPTION_COUNTER_NAME = METRICS_PREFIX + "data.outbound.subscriptions";
    private final String DATA_OUTBOUND_FILTERED_COUNTER_NAME = METRICS_PREFIX + "data.outbound.filtered";
    private final String DATA_SUBSCRIPTION_ADDED_COUNTER_NAME = METRICS_PREFIX + "subscription.added";
    private final String DATA_SUBSCRIPTION_REMOVED_COUNTER_NAME = METRICS_PREFIX + "subscription.removed";
    private final String DATA_SUBSCRIPTION_TOTAL_GAUGE_NAME = METRICS_PREFIX + "subscription";

    private Map<String, Gauge> subscriptionTotalGaugeByHost = Maps.newHashMap();

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

        Collection<Subscription> subscriptions = dataStorageService.getSubscriptions();

        Map<String, BigInteger> hostMap = Maps.newHashMap();

        for (Subscription subscription : subscriptions) {
            String host = subscription.getPushHost();
            if (host != null) {
                BigInteger subscriptionsByHost = hostMap.get(host);
                if (subscriptionsByHost == null) {
                    subscriptionsByHost = BigInteger.ZERO;
                }
                hostMap.put(host, subscriptionsByHost.add(BigInteger.ONE));
            }
        }

        for (String host : hostMap.keySet()) {
            totalSubscriptions(host, hostMap.get(host));
        }
    }

    public void registerIncomingData(String dataType, int count) {
        super.counter(DATA_INBOUND_COUNTER_NAME, "datatype", dataType).increment(count);
    }

    public void registerOutboundData(String dataType, int count) {
        super.counter(DATA_OUTBOUND_COUNTER_NAME, "datatype", dataType).increment(count);
    }

    public void registerDataToSubscriber(String subscriberHost, String dataType, String codespace, int count) {
        super.counter(DATA_OUTBOUND_SUBSCRIPTION_COUNTER_NAME, "datatype", dataType, "subscriber", subscriberHost, "codespace", codespace).increment(count);
    }

    public void registerAddedSubscription(String subscriberHost, int count) {
        super.counter(DATA_SUBSCRIPTION_ADDED_COUNTER_NAME, "subscriber", subscriberHost).increment(count);
    }

    public void registerRemovedSubscription(String subscriberHost, int count) {
        super.counter(DATA_SUBSCRIPTION_REMOVED_COUNTER_NAME, "subscriber", subscriberHost).increment(count);
    }

    public void totalSubscriptions(String subscriberHost, BigInteger count) {
        if (count.intValue() > 0) {
            if (!subscriptionTotalGaugeByHost.containsKey(subscriberHost)) {
                subscriptionTotalGaugeByHost.put(subscriberHost, Gauge.builder(DATA_SUBSCRIPTION_TOTAL_GAUGE_NAME, count, BigInteger::doubleValue).tags("subscriber", subscriberHost).register(this));
            }
            Gauge gauge = subscriptionTotalGaugeByHost.get(subscriberHost);
            if (gauge instanceof io.prometheus.client.Gauge) {
                ((io.prometheus.client.Gauge) gauge).clear();
                ((io.prometheus.client.Gauge) gauge).set(count.doubleValue());
            }
        }
    }
}
