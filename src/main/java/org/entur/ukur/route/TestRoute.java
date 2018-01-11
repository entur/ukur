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

import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class TestRoute extends RouteBuilder {

    public static final String MORE_DATA_HEADER = "moreData";

    @Override
    public void configure() {
        String outfolder = "target/received/"+System.currentTimeMillis();
        SubscriptionManager subscriptionManager = new SubscriptionManager();
        addTestSubscriptions(subscriptionManager);

        UUID uuid = UUID.randomUUID();
        String siriETurl = "https4://api.entur.org/anshar/1.0/rest/et?requestorId=" + uuid;
        String siriSXurl = "https4://api.entur.org/anshar/1.0/rest/sx?requestorId=" + uuid;

        int repatInterval = 60_000;

        from("quartz2://avvik/test?fireNow=true&trigger.repeatInterval=" + repatInterval)
                .routeId("Quartz trigger")
                .log("Trigget av timer!")
                .to("direct:retrieveAnsharET")
//                .to("direct:retrieveAnsharSX")
                ;

        from("direct:retrieveAnsharET")
                .routeId("Anshar ET retriever")
                .log("About to call Anshar with url: " + siriETurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriETurl)
//                .process(new SimpleETProcessor(outfolder))
                .process(new NsbETSubscriptionProcessor(subscriptionManager))
                .choice()
                    .when(header(MORE_DATA_HEADER).isEqualTo(true))
                        .log("Call Anshar again since there are more ET data")
                        .to("direct:retrieveAnsharET")
                    .otherwise()
                        .log("No more ET data from Anshar now")
                .endChoice()
                .end();

        from("direct:retrieveAnsharSX")
                .routeId("Anshar SX retriever")
                .log("About to call Anshar with url: " + siriSXurl)
                .setHeader(Exchange.HTTP_METHOD, constant("GET"))
                .to(siriSXurl)
//                .process(new SimpleSXProcessor(outfolder))
                .process(new NsbSXSubscriptionProcessor(subscriptionManager))
                .choice()
                    .when(header(MORE_DATA_HEADER).isEqualTo(true))
                        .log("Call Anshar again since there are more SX data")
                        .to("direct:retrieveAnsharSX")
                    .otherwise()
                        .log("No more SX data from Anshar now")
                .endChoice()
                .end();
    }

    private void addTestSubscriptions(SubscriptionManager subscriptionManager) {
        //TODO: Vi trenger noen test susbcriptions... Hardkodes her i første omgang!
        Subscription askerTilOslo = new Subscription();
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

        Subscription osloTilAsker = new Subscription();
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