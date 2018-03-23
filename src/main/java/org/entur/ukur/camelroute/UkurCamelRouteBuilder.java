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
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.IMap;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Predicate;
import org.apache.camel.Route;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.language.XPathExpression;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spring.SpringRouteBuilder;
import org.entur.ukur.camelroute.policy.InterruptibleHazelcastRoutePolicy;
import org.entur.ukur.camelroute.status.RouteStatus;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.subscription.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.entur.ukur.camelroute.policy.SingletonRoutePolicyFactory.SINGLETON_ROUTE_DEFINITION_GROUP_NAME;

@Component
public class UkurCamelRouteBuilder extends SpringRouteBuilder {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    public static final String ROUTE_ET_RETRIEVER = "seda:retrieveAnsharET";
    public static final String ROUTE_SX_RETRIEVER = "seda:retrieveAnsharSX";
    private static final String ROUTE_FLUSHJOURNEYS = "seda:flushOldJourneys";
    private static final String ROUTE_TIAMAT_MAP = "seda:getStopPlacesAndQuays";
    private static final String ROUTEID_SX_RETRIEVER = "SX Retriever";
    private static final String ROUTEID_ET_RETRIEVER = "ET Retriever";
    private static final String ROUTEID_TIAMAT_MAP = "Tiamat StopPlacesAndQuays";
    private static final String ROUTEID_FLUSHJOURNEYS = "Flush Old Journeys Asynchronously";
    private static final String ROUTEID_FLUSHJOURNEYS_TRIGGER = "Flush Old Journeys";
    private static final String ROUTEID_ET_TRIGGER = "ET trigger";
    private static final String ROUTEID_SX_TRIGGER = "SX trigger";
    private static final String ROUTEID_TIAMAT_MAP_TRIGGER = "Tiamat trigger";
    private static final String MORE_DATA = "MoreData";
    private UkurConfiguration config;
    private final NsbETSubscriptionProcessor nsbETSubscriptionProcessor;
    private NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor;
    private IMap<String, String> sharedProperties;
    private MetricsService metricsService;
    private String nodeStarted;
    private TiamatStopPlaceQuaysProcessor tiamatStopPlaceQuaysProcessor;

    @Autowired
    public UkurCamelRouteBuilder(UkurConfiguration config,
                                 NsbETSubscriptionProcessor nsbETSubscriptionProcessor,
                                 NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor,
                                 TiamatStopPlaceQuaysProcessor tiamatStopPlaceQuaysProcessor,
                                 IMap<String, String> sharedProperties,
                                 MetricsService metricsService) {
        this.config = config;
        this.nsbETSubscriptionProcessor = nsbETSubscriptionProcessor;
        this.nsbSXSubscriptionProcessor = nsbSXSubscriptionProcessor;
        this.tiamatStopPlaceQuaysProcessor = tiamatStopPlaceQuaysProcessor;
        this.sharedProperties = sharedProperties;
        this.metricsService = metricsService;
        nodeStarted = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void configure() {
        String proposedValue = "ukur-" + UUID.randomUUID();
        String requestorId = sharedProperties.putIfAbsent("AnsharRequestorId", proposedValue);
        requestorId = requestorId == null ? proposedValue : requestorId;
        logger.debug("Uses requestorId: '{}' - proposed value was {}", requestorId, proposedValue);
        String siriETurl = config.getAnsharETCamelUrl(requestorId);
        String siriSXurl = config.getAnsharSXCamelUrl(requestorId);

        createWorkerRoutes(siriETurl, siriSXurl, config.getTiamatStopPlaceQuaysURL());
        createRestRoutes(config.getRestPort(), config.isEtPollingEnabled(), config.isSxPollingEnabled());
        createQuartzRoutes(config.isEtPollingEnabled(), config.isSxPollingEnabled(), config.getPollingInterval(), config.isTiamatStopPlaceQuaysEnabled(), config.getTiamatStopPlaceQuaysInterval());
    }

    private void createRestRoutes(int jettyPort, boolean etPollingEnabled, boolean sxPollingEnabled) {
        restConfiguration()
                .component("jetty")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .port(jettyPort);

        rest("/health")
                .get("/subscriptions").to("bean:subscriptionManager?method=listAll")
                .get("/routes").to("direct:routeStatus")
                .get("/live").to("direct:OK")
                .get("/ready").to("direct:OK");

        rest("/journeys")
                .get("/").to("bean:liveRouteManager?method=getJourneys()")
                .get("/{lineref}/").to("bean:liveRouteManager?method=getJourneys(${header.lineref})");

        rest("/subscription")
                .post().type(Subscription.class).outType(Subscription.class).to("bean:subscriptionManager?method=add(${body})")
                .delete("{id}").to("bean:subscriptionManager?method=remove(${header.id})");

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
                    status.setHostname(InetAddress.getLocalHost().getHostName());
                    status.setStatusJourneyFlush(routeStatus(ROUTEID_FLUSHJOURNEYS_TRIGGER, null));
                    status.setStatusETPolling(routeStatus(ROUTEID_ET_TRIGGER, etPollingEnabled));
                    status.setStatusSXPolling(routeStatus(ROUTEID_SX_TRIGGER, sxPollingEnabled));
                    for (Map.Entry<String, Meter> entry : metricsService.getMeters().entrySet()) {
                        status.addMeter(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Timer> entry : metricsService.getTimers().entrySet()) {
                        status.addTimer(entry.getKey(), entry.getValue());
                    }
                    for (Map.Entry<String, Gauge> entry : metricsService.getGauges().entrySet()) {
                        status.addGauge(entry.getKey(), entry.getValue());
                    }
                    exchange.getOut().setBody(status);
                });
    }

    private void createQuartzRoutes(boolean etPollingEnabled, boolean sxPollingEnabled, int repatInterval, boolean stopPlaceToQuayEnabled, int tiamatRepatInterval) {

        if (etPollingEnabled) {
            createSingletonQuartz2Route("pollAnsharET", repatInterval, ROUTEID_ET_TRIGGER, ROUTEID_ET_RETRIEVER, ROUTE_ET_RETRIEVER);
        } else {
            logger.warn("ET polling is disabled");
        }

        if (sxPollingEnabled) {
            createSingletonQuartz2Route("pollAnsharSX", repatInterval, ROUTEID_SX_TRIGGER, ROUTEID_SX_RETRIEVER, ROUTE_SX_RETRIEVER);
        } else {
            logger.warn("SX polling is disabled");
        }

        createSingletonQuartz2Route("flushOldJourneys", repatInterval, ROUTEID_FLUSHJOURNEYS_TRIGGER, ROUTEID_FLUSHJOURNEYS, ROUTE_FLUSHJOURNEYS);

        if (stopPlaceToQuayEnabled) {
            from("quartz2://ukur/getStopPlacesFromTiamat?trigger.repeatInterval=" + tiamatRepatInterval + "&fireNow=true")
                    .routeId(ROUTEID_TIAMAT_MAP_TRIGGER)
                    .filter(e -> isNotRunning(ROUTEID_TIAMAT_MAP))
                    .log(LoggingLevel.DEBUG, "getStopPlacesFromTiamat triggered by timer")
                    .to(ROUTE_TIAMAT_MAP);
        }

    }

    private void createSingletonQuartz2Route(String timerName, int repatInterval, String triggerRouteId, String toRouteId, String toRoute) {
        String uri = "quartz2://ukur/" + timerName + "?trigger.repeatInterval=" + repatInterval + "&startDelayedSeconds=5&fireNow=true";
        singletonFrom(uri, triggerRouteId)
                .filter(e -> isLeader(e.getFromRouteId()))
                .filter(e -> isNotRunning(toRouteId))
                .log(LoggingLevel.DEBUG, timerName + " triggered by timer")
                .to(toRoute);
    }

    private void createWorkerRoutes(String siriETurl, String siriSXurl, String tiamatStopPlaceQuaysURL) {

        Predicate splitComplete = exchangeProperty(Exchange.SPLIT_COMPLETE).isEqualTo(true);
        Predicate moreData = exchangeProperty(MORE_DATA).isEqualToIgnoreCase("true");
        Predicate callAnsharAgain = PredicateBuilder.and(splitComplete, moreData);

        Namespaces ns = new Namespaces("s", "http://www.siri.org.uk/siri");
        XPathExpression moreDataExpression = ns.xpath("/s:Siri/s:ServiceDelivery/s:MoreData/text()", String.class);

        from(ROUTE_ET_RETRIEVER)
                .routeId(ROUTEID_ET_RETRIEVER)
                .to("metrics:timer:" + MetricsService.TIMER_ET_PULL + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriETurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriETurl)
                .convertBodyTo(Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .to("metrics:timer:" + MetricsService.TIMER_ET_PULL + "?action=stop")
                //TODO: this only selects elements with NSB as operator
                .split(ns.xpath("//s:EstimatedVehicleJourney[s:OperatorRef/text()='NSB']"))
                .bean(metricsService, "registerSentMessage('EstimatedVehicleJourney')")
                .to("activemq:queue:" + UkurConfiguration.ET_QUEUE)
                .choice()
                .when(callAnsharAgain)
                .log(LoggingLevel.DEBUG, "Call Anshar again since there are more ET data")
                .to(ROUTE_ET_RETRIEVER)
                .end();

        from("activemq:queue:" + UkurConfiguration.ET_QUEUE)
                .routeId("ET ActiveMQ Listener")
                .process(nsbETSubscriptionProcessor)
                .end();

        from(ROUTE_SX_RETRIEVER)
                .routeId(ROUTEID_SX_RETRIEVER)
                .to("metrics:timer:" + MetricsService.TIMER_SX_PULL + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriSXurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriSXurl)
                .convertBodyTo(Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .to("metrics:timer:" + MetricsService.TIMER_SX_PULL + "?action=stop")
                //TODO: this only selects elements with NSB as participant
                .split(ns.xpath("//s:PtSituationElement[s:ParticipantRef/text()='NSB']"))
                .bean(metricsService, "registerSentMessage('PtSituationElement')")
                .to("activemq:queue:" + UkurConfiguration.SX_QUEUE)
                .choice()
                .when(callAnsharAgain)
                .log(LoggingLevel.DEBUG, "Call Anshar again since there are more SX data")
                .to(ROUTE_SX_RETRIEVER)
                .end();

        from("activemq:queue:" + UkurConfiguration.SX_QUEUE)
                .routeId("SX ActiveMQ Listener")
                .process(nsbSXSubscriptionProcessor)
                .end();

        from(ROUTE_FLUSHJOURNEYS)
                .routeId(ROUTEID_FLUSHJOURNEYS)
                .to("bean:liveRouteManager?method=flushOldJourneys()");

        from(ROUTE_TIAMAT_MAP)
                .routeId(ROUTEID_TIAMAT_MAP)
                .to("metrics:timer:" + MetricsService.TIMER_TIAMAT + "?action=start")
                .log(LoggingLevel.DEBUG, "About to call Tiamat with url: " + tiamatStopPlaceQuaysURL)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(tiamatStopPlaceQuaysURL)
                .process(tiamatStopPlaceQuaysProcessor)
                .to("metrics:timer:" + MetricsService.TIMER_TIAMAT + "?action=stop")
                .end();
    }

    private String routeStatus(String routeidSxTrigger, Boolean enabled) {
        String s = isLeader(routeidSxTrigger) ? "LEADER" : "NOT LEADER";
        if (Boolean.FALSE.equals(enabled)) {
            s += " (disabled)";
        }
        return s;
    }

    /**
     * Create a new singleton camelroute definition from URI. Only one such camelroute should be active throughout the cluster at any time.
     */
    private RouteDefinition singletonFrom(String uri, String routeId) {
        return this.from(uri)
                .group(SINGLETON_ROUTE_DEFINITION_GROUP_NAME)
                .routeId(routeId)
                .autoStartup(true);
    }


    private boolean isLeader(String routeId) {
        Route route = getContext().getRoute(routeId);
        if (route != null) {
            RouteContext routeContext = route.getRouteContext();
            List<RoutePolicy> routePolicyList = routeContext.getRoutePolicyList();
            if (routePolicyList != null) {
                for (RoutePolicy routePolicy : routePolicyList) {
                    if (routePolicy instanceof InterruptibleHazelcastRoutePolicy) {
                        return ((InterruptibleHazelcastRoutePolicy) (routePolicy)).isLeader();
                    }
                }
            }
        }
        return false;
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