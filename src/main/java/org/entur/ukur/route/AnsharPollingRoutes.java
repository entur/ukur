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

import com.hazelcast.core.IMap;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Predicate;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.xml.Namespaces;
import org.apache.camel.model.language.XPathExpression;
import org.apache.camel.model.rest.RestBindingMode;
import org.entur.ukur.setup.UkurConfiguration;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.UUID;

@Component
public class AnsharPollingRoutes extends AbstractClusterRouteBuilder {
    public static final String ROUTE_ET_RETRIEVER = "seda:retrieveAnsharET";
    public static final String ROUTE_SX_RETRIEVER = "seda:retrieveAnsharSX";
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String ROUTEID_SX_RETRIEVER = "SX Retriever";
    private static final String ROUTEID_ET_RETRIEVER = "ET Retriever";
    private static final String MORE_DATA = "MoreData";
    private static final String ROUTEID_ET_TRIGGER = "ET trigger";
    private static final String ROUTEID_SX_TRIGGER = "SX trigger";
    private UkurConfiguration config;
    private final NsbETSubscriptionProcessor nsbETSubscriptionProcessor;
    private NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor;
    private IMap<String, String> sharedProperties;
    private SubscriptionManager subscriptionManager;

    @Autowired
    public AnsharPollingRoutes(UkurConfiguration config,
                               NsbETSubscriptionProcessor nsbETSubscriptionProcessor,
                               NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor,
                               IMap<String, String> sharedProperties,
                               SubscriptionManager subscriptionManager) {
        this.config = config;
        this.nsbETSubscriptionProcessor = nsbETSubscriptionProcessor;
        this.nsbSXSubscriptionProcessor = nsbSXSubscriptionProcessor;
        this.sharedProperties = sharedProperties;
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    public void configure() {
        String proposedValue = "ukur-" + UUID.randomUUID();
        String requestorId = sharedProperties.putIfAbsent("AnsharRequestorId", proposedValue);
        requestorId = requestorId == null ? proposedValue : requestorId;
        logger.debug("Uses requestorId: '{}' - proposded value was {}", requestorId, proposedValue);
        String siriETurl = config.getAnsharETCamelUrl(requestorId);
        String siriSXurl = config.getAnsharSXCamelUrl(requestorId);

        createPollingRoutes(siriETurl, siriSXurl);
        createRestRoutes(config.getRestPort());
        createQuartzRoutes(config.isEtPollingEnabled(), config.isSxPollingEnabled(), config.getPollingInterval());

    }

    private void createRestRoutes(int jettyPort) {
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
                    status.setHostname(InetAddress.getLocalHost().getHostName());
                    status.setLeaderForETPolling(isLeader(ROUTEID_ET_TRIGGER));
                    status.setLeaderForSXPolling(isLeader(ROUTEID_SX_TRIGGER));
                    status.setEtSubscriptionStatus(nsbETSubscriptionProcessor.getStatus());
                    status.setSxSubscriptionStatus(nsbSXSubscriptionProcessor.getStatus());
                    status.setNumberOfSubscriptions(subscriptionManager.getNoSubscriptions());
                    exchange.getOut().setBody(status);
                });
    }

    private void createQuartzRoutes(boolean etPollingEnabled, boolean sxPollingEnabled, int repatInterval) {

        if (etPollingEnabled) {
            singletonFrom("quartz2://ukur/pollAnsharET?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_ET_TRIGGER)
                    .filter(e -> isLeader(e.getFromRouteId()))
                    .filter(new NotRunningPredicate(ROUTEID_ET_RETRIEVER))
                    .log(LoggingLevel.DEBUG, "ET: Triggered by timer")
                    .to(ROUTE_ET_RETRIEVER);
        } else {
            logger.warn("ET polling is disabled");
        }

        if (sxPollingEnabled) {
            singletonFrom("quartz2://ukur/pollAnsharSX?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_SX_TRIGGER)
                    .filter(e -> isLeader(e.getFromRouteId()))
                    .filter(new NotRunningPredicate(ROUTEID_SX_RETRIEVER))
                    .log(LoggingLevel.DEBUG, "SX: Triggered by timer")
                    .to(ROUTE_SX_RETRIEVER);
        } else {
            logger.warn("SX polling is disabled");
        }
    }

    private void createPollingRoutes(String siriETurl, String siriSXurl) {

        Predicate splitComplete = exchangeProperty(Exchange.SPLIT_COMPLETE).isEqualTo(true);
        Predicate moreData = exchangeProperty(MORE_DATA).isEqualToIgnoreCase("true");
        Predicate callAnsharAgain = PredicateBuilder.and(splitComplete, moreData);

        Namespaces ns = new Namespaces("s", "http://www.siri.org.uk/siri");
        XPathExpression moreDataExpression = ns.xpath("/s:Siri/s:ServiceDelivery/s:MoreData/text()", String.class);

        from(ROUTE_ET_RETRIEVER)
                .routeId(ROUTEID_ET_RETRIEVER)
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriETurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriETurl)
                .convertBodyTo(org.w3c.dom.Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .split(ns.xpath("//s:EstimatedVehicleJourney"))
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
                .log(LoggingLevel.DEBUG, "About to call Anshar with url: " + siriSXurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriSXurl)
                .convertBodyTo(org.w3c.dom.Document.class)
                .setProperty(MORE_DATA, moreDataExpression)
                .split(ns.xpath("//s:PtSituationElement"))
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
    }

}