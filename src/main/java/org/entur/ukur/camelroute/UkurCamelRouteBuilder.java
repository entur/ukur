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

package org.entur.ukur.camelroute;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.http.entity.ContentType;
import org.entur.protobuf.mapper.SiriMapper;
import org.entur.ukur.camelroute.status.RouteStatus;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.service.PrometheusMetricsService;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.xml.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import uk.org.siri.siri20.Siri;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.camel.Exchange.CONTENT_LENGTH;

@Component
public class UkurCamelRouteBuilder extends SpringRouteBuilder {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
            static final String ROUTE_ET_RETRIEVER = "seda:retrieveAnsharET";
            static final String ROUTE_SX_RETRIEVER = "seda:retrieveAnsharSX";
    private static final String ROUTE_TIAMAT_MAP = "seda:getStopPlacesAndQuays";
    private static final String ROUTEID_TIAMAT_MAP = "Tiamat StopPlacesAndQuays";
    private static final String ROUTEID_HEARTBEAT_CHECKER  = "Check Subscriptions For Missing Heartbeats";
    private static final String ROUTEID_HEARTBEAT_TRIGGER  = "Check Subscriptions Trigger";
    private static final String ROUTEID_ET_TRIGGER = "ET trigger";
    private static final String ROUTEID_SX_TRIGGER = "SX trigger";
    private static final String ROUTEID_TIAMAT_MAP_TRIGGER = "Tiamat trigger";
    private static final String ROUTEID_ANSHAR_SUBSRENEWER_TRIGGER = "Anshar Subscription Renewer Trigger";
    private static final String ROUTEID_ANSHAR_SUBSCHECKER_TRIGGER = "Anshar Subscription Checker Trigger";

    private final UkurConfiguration config;
    private final ETSubscriptionProcessor ETSubscriptionProcessor;
    private final SXSubscriptionProcessor SXSubscriptionProcessor;
    private final MetricsService metricsService;

    private final PrometheusMetricsService prometheusMeterRegistry;

    private final String nodeStarted;
    private final TiamatStopPlaceQuaysProcessor tiamatStopPlaceQuaysProcessor;
    private final Namespaces siriNamespace = new Namespaces("s", "http://www.siri.org.uk/siri");
    private final KryoSerializer kryoSerializer = new KryoSerializer();

    @Autowired
    public UkurCamelRouteBuilder(UkurConfiguration config,
                                 ETSubscriptionProcessor ETSubscriptionProcessor,
                                 SXSubscriptionProcessor SXSubscriptionProcessor,
                                 TiamatStopPlaceQuaysProcessor tiamatStopPlaceQuaysProcessor,
                                 MetricsService metricsService, PrometheusMetricsService prometheusMeterRegistry) {
        this.config = config;
        this.ETSubscriptionProcessor = ETSubscriptionProcessor;
        this.SXSubscriptionProcessor = SXSubscriptionProcessor;
        this.tiamatStopPlaceQuaysProcessor = tiamatStopPlaceQuaysProcessor;
        this.metricsService = metricsService;
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        nodeStarted = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        siriNamespace.add("ns2", "http://www.ifopt.org.uk/acsb");
    }

    @Override
    public void configure() {
        onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.WARN, "Caught ${exception}")
                .transform().simple("${exception.message}");

        onException(InvalidSubscriptionIdException.class)
                .handled(true)
                // use HTTP status 400 when input data is invalid
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setBody(constant(""));

        createJaxbProcessingRoutes();
        createWorkerRoutes(config.getTiamatStopPlaceQuaysURL());
        createRestRoutes(config.getRestPort());
        createQuartzRoutes(config.getHeartbeatCheckInterval(), config.isTiamatStopPlaceQuaysEnabled(), config.getTiamatStopPlaceQuaysInterval());
    }

    private void createJaxbProcessingRoutes() {



        from("direct:compress.jaxb")
                .setBody(body().convertToString())
                .bean(kryoSerializer, "write")
        ;

        from("direct:decompress.jaxb")
                .bean(kryoSerializer, "read")
                .process(p -> {
                    final String body = p.getIn().getBody(String.class);
                    p.getOut().setBody(body);
                    p.getOut().setHeaders(p.getIn().getHeaders());
                    p.getOut().setHeader(CONTENT_LENGTH, body.getBytes().length);
                })
        ;

        from("direct:map.protobuf.to.jaxb")
                .process(p -> {
                    p.getOut().setBody(p.getIn().getBody(byte[].class));
                    p.getOut().setHeaders(p.getIn().getHeaders());
                })
                .bean(SiriMapper.class, "mapToJaxb")
                .process(p -> {
                    final Siri body = p.getIn().getBody(Siri.class);
                    p.getOut().setBody(body);
                    p.getOut().setHeaders(p.getIn().getHeaders());
                })
        ;
    }

    private void createWorkerRoutes(String tiamatStopPlaceQuaysURL) {

        from(config.getEtPubsubQueue())
                .routeId("ET pubsub Listener")
                .log(LoggingLevel.DEBUG, "About to handle ET message from queue")
                .to("direct:map.protobuf.to.jaxb")
                .process(ETSubscriptionProcessor)
                .log(LoggingLevel.DEBUG, "Done handling ET message from queue")
                .end();

        from(config.getSxPubsubQueue())
                .routeId("SX pubsub Listener")
                .log(LoggingLevel.DEBUG, "About to handle SX message from queue")
                .to("direct:map.protobuf.to.jaxb")
                .process(SXSubscriptionProcessor)
                .log(LoggingLevel.DEBUG, "Done handling SX message from queue")
                .end();

        from(ROUTE_TIAMAT_MAP)
                .routeId(ROUTEID_TIAMAT_MAP)
                .to("metrics:timer:" + MetricsService.TIMER_TIAMAT + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Tiamat with url: " + tiamatStopPlaceQuaysURL)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .setHeader("ET-Client-Name", constant("Ukur"))
                .setHeader("ET-Client-ID", constant(getHostName()))
                .to(tiamatStopPlaceQuaysURL)
                .process(tiamatStopPlaceQuaysProcessor)
                .to("metrics:timer:" + MetricsService.TIMER_TIAMAT + "?action=stop")
                .end();

    }

    private void createRestRoutes(int jettyPort) {
        restConfiguration()
                .component("jetty")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .port(jettyPort)
                .contextPath("/")
                .apiContextPath("/external/swagger.json")
                .apiProperty("api.title", "Ukur API")
                .apiProperty("api.description", "Ukur offers subscriptions to deviations in traffic. This API doc describes " +
                        "both external and internal api's, but only the external are reachable from Internet (without external " +
                        "as part of the url).")
                .apiProperty("api.version", "v1.0.1")
                .apiProperty("cors", "true");

        rest("/internal/health")
                .bindingMode(RestBindingMode.json)
                .get("/subscriptions").to("bean:subscriptionManager?method=listAll")
                .get("/subscriptions/reload").to("bean:subscriptionManager?method=reloadSubscriptionCache")
                .get("/routes").to("direct:routeStatus")
                .get("/live").to("direct:OK")
                .get("/ready").to("direct:ready")
                .get("/scrape").to("direct:scrape");

        rest("/external/subscription")
                .bindingMode(RestBindingMode.json)
                .post().type(Subscription.class).outType(Subscription.class).to("bean:subscriptionManager?method=addOrUpdate(${body})")
                .delete("{id}").to("bean:subscriptionManager?method=remove(${header.id})");

        rest("/external/siri-subscription")
                .bindingMode(RestBindingMode.xml)
                .post().type(Siri.class).outType(Siri.class).to("bean:siriXMLSubscriptionHandler?method=handle(${body}, null)")
                .post("{codespace}").type(Siri.class).outType(Siri.class).to("bean:siriXMLSubscriptionHandler?method=handle(${body}, ${header.codespace})");

        from("direct:scrape")
                .routeId("Prometheus scrape")
                .bean(prometheusMeterRegistry, "scrape")
                .setHeader(Exchange.CONTENT_TYPE, constant(ContentType.TEXT_PLAIN))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"));

        from("direct:ready")
                .routeId("Ready checker")
                .choice()
                .when(exchange -> tiamatStopPlaceQuaysProcessor.hasRun()).to("direct:OK")
                .otherwise()
                .log(LoggingLevel.WARN, "not ready (has not retrieved stopplace data yet)")
                .setBody(simple("NOT OK    \n\n"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("500"));

        from("direct:OK")
                .routeId("OK response")
                .log(LoggingLevel.TRACE, "Return hardcoded 'OK' on uri '${header." + Exchange.HTTP_URI + "}'")
                .setBody(simple("OK    \n\n"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"));

        from("direct:routeStatus")
                .routeId("Route Status")
                .process(exchange -> {
                    RouteStatus status = new RouteStatus();
                    status.setNodeStartTime(nodeStarted);
                    status.setHostname(getHostName());
                    for (Map.Entry<String, Meter> entry : metricsService.getMeters().entrySet()) {
                        status.addMeter(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Timer> entry : metricsService.getTimers().entrySet()) {
                        status.addTimer(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Gauge> entry : metricsService.getGauges().entrySet()) {
                        status.addGauge(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Histogram> entry : metricsService.getHistograms().entrySet()) {
                        status.addHistogram(entry.getKey(), entry.getValue());
                    }
                    exchange.getOut().setBody(status);
                });

    }

    private void createQuartzRoutes(int subscriptionCheckerRepatInterval, boolean stopPlaceToQuayEnabled, int tiamatRepatInterval) {

        if (stopPlaceToQuayEnabled) {
            from("quartz2://ukur/getStopPlacesFromTiamat?trigger.repeatInterval=" + tiamatRepatInterval + "&fireNow=true")
                    .routeId(ROUTEID_TIAMAT_MAP_TRIGGER)
                    .filter(e -> isNotRunning(ROUTEID_TIAMAT_MAP))
                    .log(LoggingLevel.DEBUG, "getStopPlacesFromTiamat triggered by timer")
                    .to(ROUTE_TIAMAT_MAP);
        }
    }

    private String getHostName(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "Ukur-UnknownHost";
        }
    }

    private boolean isNotRunning(String routeId) {
        int size = getContext().getInflightRepository().size(routeId);
        boolean notRunning = size == 0;
        logger.trace("Number of running instances of camelroute '{}' is {} - returns {}", routeId, size, notRunning);
        return notRunning;
    }

    @Bean(name = "json-jackson")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public JacksonDataFormat jacksonDataFormat(ObjectMapper objectMapper) {
        return new JacksonDataFormat(objectMapper, HashMap.class);
    }

}