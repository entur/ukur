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

package org.entur.ukur.service;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Strings;
import org.apache.camel.component.metrics.MetricsComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
@Configuration
public class MetricsService {
    public static final String TIMER_PUSH    = "timer.pushToHttp";
    public static final String TIMER_ET_PULL = "timer.pullETFromAnshar";
    public static final String TIMER_SX_PULL = "timer.pullSXFromAnshar";
    public static final String GAUGE_SUBSCRIPTIONS = "gauge.subscriptions";
    public static final String GAUGE_LIVE_JOURNEYS = "gauge.liveJourneys";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MetricRegistry metrics = new MetricRegistry();
    private final boolean graphiteEnabled;
    private HashSet<String> uniqueMeters = new HashSet<>();
    private GraphiteReporter reporter;
    private Graphite graphite;

    /**
     * Bean factory method so we can use this metricregistry directly in camel routes.
     */
    @Bean(name = MetricsComponent.METRIC_REGISTRY_NAME)
    public MetricRegistry getMetricRegistry() {
        return metrics;
    }

    @Autowired
    public MetricsService(@Value("${ukur.graphite.host:}") String graphiteHost,
                          @Value("${ukur.graphite.port:2003}") int graphitePort) {

        if (Strings.isNullOrEmpty(graphiteHost) ) {
            graphiteEnabled = false;
            logger.info("Setting up local metrics service");
        } else {
            graphiteEnabled = true;
            logger.info("Setting up metrics service with graphite server: host={} port={}", graphiteHost, graphitePort);
            graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));
            reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith("app.ukur")
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);
        }
    }

    @PostConstruct
    public void startGraphiteReporter() {
        if (graphiteEnabled) {
            logger.info("Starting graphite reporter");
            reporter.start(10, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void shutdownGraphiteReporter() throws IOException {
        if (graphiteEnabled) {
            reporter.stop();
            if (graphite.isConnected()) {
                graphite.flush();
                graphite.close();
            }
        }
    }

    @SuppressWarnings("unused") //Used directly from Camel route
    public void registerSentMessage(String messagetype) {
        String counterName = "message.sent." + messagetype;
        uniqueMeters.add(counterName);
        metrics.meter(counterName).mark();
    }

    public void registerReceivedMessage(Class messageClass) {
        String counterName = "message.received." + messageClass.getSimpleName();
        uniqueMeters.add(counterName);
        metrics.meter(counterName).mark();
    }

    public void registerHandledMessage(Class messageClass) {
        String counterName = "message.handled." + messageClass.getSimpleName();
        uniqueMeters.add(counterName);
        metrics.meter(counterName).mark();
    }

    public Set<String> getMeterNames() {
        return Collections.unmodifiableSet(uniqueMeters);
    }

    public Meter getMeter(String meterName) {
        if (uniqueMeters.contains(meterName)) {
            return metrics.meter(meterName);
        }
        return null;
    }

    public Timer getTimer(String name) {
        return metrics.timer(name);
    }

    public void registerGauge(String name, Gauge<Integer> gauge) {
        metrics.register(name, gauge);
    }
}
