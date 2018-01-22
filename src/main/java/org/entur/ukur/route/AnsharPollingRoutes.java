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
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String MORE_DATA = "MoreData";
    private static final String ROUTENAME_ET_TRIGGER = "ET trigger";
    private static final String ROUTENAME_SX_TRIGGER = "SX trigger";
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
        String siriETurl = config.getAnsharETCamelUrl(uuid);
        String siriSXurl = config.getAnsharSXCamelUrl(uuid);

        createPollingRoutes(siriETurl, siriSXurl);
        createRestRoutes();
        if (config.isQuartzRoutesEnabled()) {
            createQuartzRoutes();
        } else {
            logger.warn("Quartz routes disabled!");
        }

    }

    protected void createRestRoutes() {
        restConfiguration()
                .component("jetty")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .port(8080);
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
                .setBody(simple("OK"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"));
        from("direct:routeStatus-et")
                .choice()
                    .when(p -> isLeader(ROUTENAME_ET_TRIGGER))
                        .setBody(simple("Is leader for "+ ROUTENAME_ET_TRIGGER))
                    .otherwise()
                        .setBody(simple("Is NOT leader for "+ ROUTENAME_ET_TRIGGER))
                .end();
        from("direct:routeStatus-sx")
                .choice()
                    .when(p -> isLeader(ROUTENAME_SX_TRIGGER))
                        .setBody(simple("Is leader for "+ ROUTENAME_SX_TRIGGER))
                    .otherwise()
                        .setBody(simple("Is NOT leader for "+ ROUTENAME_SX_TRIGGER))
                .end();
    }

    protected void createQuartzRoutes() {
        int repatInterval = 60_000;

        singletonFrom("quartz2://ukur/pollAnsharET?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTENAME_ET_TRIGGER)
                .filter(e -> isLeader(e.getFromRouteId()))
                .to("direct:retrieveAnsharET");

        singletonFrom("quartz2://ukur/pollAnsharSX?fireNow=true&trigger.repeatInterval=" + repatInterval, ROUTENAME_SX_TRIGGER)
                .filter(e -> isLeader(e.getFromRouteId()))
                .to("direct:retrieveAnsharSX");
    }

    protected void createPollingRoutes(String siriETurl, String siriSXurl) {

        Predicate splitComplete = exchangeProperty(Exchange.SPLIT_COMPLETE).isEqualTo(true);
        Predicate moreData = exchangeProperty(MORE_DATA).isEqualToIgnoreCase("true");
        Predicate callAnsharAgain = PredicateBuilder.and(splitComplete, moreData);

        Namespaces ns = new Namespaces("s", "http://www.siri.org.uk/siri");
        XPathExpression moreDataExpression = ns.xpath("/s:Siri/s:ServiceDelivery/s:MoreData/text()", String.class);

        from("direct:retrieveAnsharET")
                .routeId("ET retriever")
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
                        .to("direct:retrieveAnsharET")
                .end();

        from("activemq:queue:"+UkurConfiguration.ET_QUEUE)
                .process(nsbETSubscriptionProcessor)
                .end();


        from("direct:retrieveAnsharSX")
                .routeId("SX retriever")
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
                        .to("direct:retrieveAnsharSX")
                .end();

        from("activemq:queue:"+UkurConfiguration.SX_QUEUE)
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