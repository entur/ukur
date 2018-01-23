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

import org.apache.camel.Exchange;
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
    private SubscriptionManager subscriptionManager;
    private final NsbETSubscriptionProcessor nsbETSubscriptionProcessor;
    private NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor;

    @Autowired
    public AnsharPollingRoutes(UkurConfiguration config,
                               SubscriptionManager subscriptionManager,
                               NsbETSubscriptionProcessor nsbETSubscriptionProcessor,
                               NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor) {
        this.config = config;
        this.subscriptionManager = subscriptionManager;
        this.nsbETSubscriptionProcessor = nsbETSubscriptionProcessor;
        this.nsbSXSubscriptionProcessor = nsbSXSubscriptionProcessor;
    }

    @Override
    public void configure() {

        addTestSubscriptions(subscriptionManager);

        UUID uuid = UUID.randomUUID();
        //TODO: Use hazelcast to get or set this uid so that we have same uid on all nodes! (use lock etc)
        String siriETurl = config.getAnsharETCamelUrl(uuid);
        String siriSXurl = config.getAnsharSXCamelUrl(uuid);

        createPollingRoutes(siriETurl, siriSXurl);
        createRestRoutes(config.getRestPort());
        createQuartzRoutes(config.isEtPollingEnabled(), config.isSxPollingEnabled(), config.getPollingInterval());

    }

    protected void createRestRoutes(int jettyPort) {
        restConfiguration()
                .component("jetty")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .port(jettyPort);
        rest("/health")
                .produces("application/json")
                .get("/subscriptions").to("bean:subscriptionManager?method=listAll")
                .get("/et").to("bean:nsbETSubscriptionProcessor?method=getStatus")
                .get("/sx").to("bean:nsbSXSubscriptionProcessor?method=getStatus")
                .get("/routes/et").to("direct:routeStatus-et")
                .get("/routes/sx").to("direct:routeStatus-sx")
                .get("/live").to("direct:OK")
                .get("/ready").to("direct:OK");
        rest("/data")
                .produces("application/json")
                .get("/{id}").to("bean:subscriptionManager?method=getData(${header.id})");

        from("direct:OK")
                .routeId("OK response")
                .log("Return hardcoded 'OK' on uri '${header."+Exchange.HTTP_URI+"}'")
                .setBody(simple("OK    \n\n"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"));
        from("direct:routeStatus-et")
                .routeId("ET Status")
                .choice()
                    .when(p -> isLeader(ROUTEID_ET_TRIGGER))
                        .setBody(simple("Is leader for route '"+ ROUTEID_ET_TRIGGER +"'"))
                    .otherwise()
                        .setBody(simple("Is NOT leader for route '"+ ROUTEID_ET_TRIGGER +"'"))
                .end();
        from("direct:routeStatus-sx")
                .routeId("SX Status")
                .choice()
                    .when(p -> isLeader(ROUTEID_SX_TRIGGER))
                        .setBody(simple("Is leader for route '"+ ROUTEID_SX_TRIGGER +"'"))
                    .otherwise()
                        .setBody(simple("Is NOT leader for route '"+ ROUTEID_SX_TRIGGER +"'"))
                .end();
    }

    protected void createQuartzRoutes(boolean etPollingEnabled, boolean sxPollingEnabled, int repatInterval) {

        if (etPollingEnabled) {
            singletonFrom("quartz2://ukur/pollAnsharET?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_ET_TRIGGER)
                    .log("ET: Triggered by timer")
                    .filter(e -> isLeader(e.getFromRouteId()))
                    .filter(e -> getContext().getInflightRepository().size(ROUTEID_ET_RETRIEVER) == 0 )
                    .to(ROUTE_ET_RETRIEVER);
        } else {
            logger.warn("ET polling is disabled");
        }

        if (sxPollingEnabled) {
            singletonFrom("quartz2://ukur/pollAnsharSX?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTEID_SX_TRIGGER)
                    .log("SX: Triggered by timer")
                    .filter(e -> isLeader(e.getFromRouteId()))
                    .filter(e -> getContext().getInflightRepository().size(ROUTEID_SX_RETRIEVER) == 0 )
                    .to(ROUTE_SX_RETRIEVER);
        } else {
            logger.warn("SX polling is disabled");
        }
    }

    protected void createPollingRoutes(String siriETurl, String siriSXurl) {

        Predicate splitComplete = exchangeProperty(Exchange.SPLIT_COMPLETE).isEqualTo(true);
        Predicate moreData = exchangeProperty(MORE_DATA).isEqualToIgnoreCase("true");
        Predicate callAnsharAgain = PredicateBuilder.and(splitComplete, moreData);

        Namespaces ns = new Namespaces("s", "http://www.siri.org.uk/siri");
        XPathExpression moreDataExpression = ns.xpath("/s:Siri/s:ServiceDelivery/s:MoreData/text()", String.class);

        from(ROUTE_ET_RETRIEVER)
                .routeId(ROUTEID_ET_RETRIEVER)
                .log("About to call Anshar with url: " + siriETurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriETurl)
                .streamCaching()
                .setProperty(MORE_DATA, moreDataExpression)
                .split(ns.xpath("//s:EstimatedVehicleJourney"))
                .to("activemq:queue:"+UkurConfiguration.ET_QUEUE)
                .choice()
                    .when(callAnsharAgain)
                        .log("Call Anshar again since there are more ET data")
                        .to(ROUTE_ET_RETRIEVER)
                .end();

        from("activemq:queue:"+UkurConfiguration.ET_QUEUE)
                .routeId("ET ActiveMQ Listener")
                .process(nsbETSubscriptionProcessor)
                .end();


        from(ROUTE_SX_RETRIEVER)
                .routeId(ROUTEID_SX_RETRIEVER)
                .log("About to call Anshar with url: " + siriSXurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriSXurl)
                .streamCaching()
                .setProperty(MORE_DATA, moreDataExpression)
                .split(ns.xpath("//s:PtSituationElement"))
                .to("activemq:queue:"+UkurConfiguration.SX_QUEUE)
                .choice()
                    .when(callAnsharAgain)
                        .log("Call Anshar again since there are more SX data")
                        .to(ROUTE_SX_RETRIEVER)
                .end();

        from("activemq:queue:"+UkurConfiguration.SX_QUEUE)
                .routeId("SX ActiveMQ Listener")
                .process(nsbSXSubscriptionProcessor)
                .end();
    }


    private void addTestSubscriptions(SubscriptionManager subscriptionManager) {
        //TODO: Vi trenger noen test susbcriptions... Hardkodes her i første omgang!
        Subscription askerTilOslo = new Subscription("1");
        askerTilOslo.setName("Asker til OsloS");
        askerTilOslo.addFromStopPoint("NSR:StopPlace:418");
        askerTilOslo.addFromStopPoint("NSR:Quay:695");
        askerTilOslo.addFromStopPoint("NSR:Quay:696");
        askerTilOslo.addFromStopPoint("NSR:Quay:697");
        askerTilOslo.addFromStopPoint("NSR:Quay:698");
        askerTilOslo.addFromStopPoint("NSR:Quay:699");
        askerTilOslo.addFromStopPoint("NSR:Quay:700");
        askerTilOslo.addToStopPoint("NSR:StopPlace:337");
        askerTilOslo.addToStopPoint("NSR:Quay:550");
        askerTilOslo.addToStopPoint("NSR:Quay:551");
        askerTilOslo.addToStopPoint("NSR:Quay:553");
        askerTilOslo.addToStopPoint("NSR:Quay:554");
        askerTilOslo.addToStopPoint("NSR:Quay:555");
        askerTilOslo.addToStopPoint("NSR:Quay:556");
        askerTilOslo.addToStopPoint("NSR:Quay:563");
        askerTilOslo.addToStopPoint("NSR:Quay:557");
        askerTilOslo.addToStopPoint("NSR:Quay:559");
        askerTilOslo.addToStopPoint("NSR:Quay:561");
        askerTilOslo.addToStopPoint("NSR:Quay:562");
        askerTilOslo.addToStopPoint("NSR:Quay:564");
        askerTilOslo.addToStopPoint("NSR:Quay:566");
        askerTilOslo.addToStopPoint("NSR:Quay:567");
        askerTilOslo.addToStopPoint("NSR:Quay:568");
        askerTilOslo.addToStopPoint("NSR:Quay:569");
        askerTilOslo.addToStopPoint("NSR:Quay:565");
        askerTilOslo.addToStopPoint("NSR:Quay:570");
        askerTilOslo.addToStopPoint("NSR:Quay:571");
        subscriptionManager.addSusbcription(askerTilOslo);

        Subscription osloTilAsker = new Subscription("2");
        osloTilAsker.setName("OsloS til Asker");
        osloTilAsker.addFromStopPoint("NSR:StopPlace:337");
        osloTilAsker.addFromStopPoint("NSR:Quay:550");
        osloTilAsker.addFromStopPoint("NSR:Quay:551");
        osloTilAsker.addFromStopPoint("NSR:Quay:553");
        osloTilAsker.addFromStopPoint("NSR:Quay:554");
        osloTilAsker.addFromStopPoint("NSR:Quay:555");
        osloTilAsker.addFromStopPoint("NSR:Quay:556");
        osloTilAsker.addFromStopPoint("NSR:Quay:563");
        osloTilAsker.addFromStopPoint("NSR:Quay:557");
        osloTilAsker.addFromStopPoint("NSR:Quay:559");
        osloTilAsker.addFromStopPoint("NSR:Quay:561");
        osloTilAsker.addFromStopPoint("NSR:Quay:562");
        osloTilAsker.addFromStopPoint("NSR:Quay:564");
        osloTilAsker.addFromStopPoint("NSR:Quay:566");
        osloTilAsker.addFromStopPoint("NSR:Quay:567");
        osloTilAsker.addFromStopPoint("NSR:Quay:568");
        osloTilAsker.addFromStopPoint("NSR:Quay:569");
        osloTilAsker.addFromStopPoint("NSR:Quay:565");
        osloTilAsker.addFromStopPoint("NSR:Quay:570");
        osloTilAsker.addFromStopPoint("NSR:Quay:571");
        osloTilAsker.addToStopPoint("NSR:StopPlace:418");
        osloTilAsker.addToStopPoint("NSR:Quay:695");
        osloTilAsker.addToStopPoint("NSR:Quay:696");
        osloTilAsker.addToStopPoint("NSR:Quay:697");
        osloTilAsker.addToStopPoint("NSR:Quay:698");
        osloTilAsker.addToStopPoint("NSR:Quay:699");
        osloTilAsker.addToStopPoint("NSR:Quay:700");
        subscriptionManager.addSusbcription(osloTilAsker);

    }


}