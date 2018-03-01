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

import com.codahale.metrics.Meter;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.App;
import org.entur.ukur.service.MetricsService;
import org.entur.ukur.subscription.SubscriptionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
@AutoConfigureWireMock(port = 0)
@SuppressWarnings("unused")
public class UkurCamelRouteBuilderTest extends AbstractJUnit4SpringContextTests {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private MetricsService metricsService;

    @Autowired
    private SubscriptionManager subscriptionManager;

    @Autowired
    private WiremockTestConfig config;

    @Produce(uri = UkurCamelRouteBuilder.ROUTE_ET_RETRIEVER)
    private ProducerTemplate etTemplate;

    @Produce(uri = UkurCamelRouteBuilder.ROUTE_SX_RETRIEVER)
    private ProducerTemplate sxTemplate;

    @Test
    public void testETpolling() throws Exception {
        wiremock("/et-pretty.xml", "/et");

        assertNull(metricsService.getMeter("message.received.EstimatedVehicleJourney"));
        assertNull(metricsService.getMeter("message.handled.EstimatedVehicleJourney"));

        etTemplate.sendBody("go!");
        waitUntil("message.handled.EstimatedVehicleJourney", 1);

        Meter received = metricsService.getMeter("message.received.EstimatedVehicleJourney");
        assertNotNull(received);
        assertEquals(1, received.getCount());
        Meter handled = metricsService.getMeter("message.handled.EstimatedVehicleJourney");
        assertNotNull(handled);
        assertEquals(1, handled.getCount());
    }

    @Test
    public void testSXpolling() throws Exception {
        wiremock("/sx-pretty.xml", "/sx");

        assertNull(metricsService.getMeter("message.received.PtSituationElement"));
        assertNull(metricsService.getMeter("message.handled.PtSituationElement"));

        sxTemplate.sendBody("go!");
        waitUntil("message.handled.PtSituationElement", 5);

        Meter received = metricsService.getMeter("message.received.PtSituationElement");
        assertNotNull(received);
        assertEquals(5, received.getCount());
        Meter handled = metricsService.getMeter("message.handled.PtSituationElement");
        assertNotNull(handled);
        assertEquals(5, handled.getCount());
    }

    private void waitUntil(String meterName, int expectedCount) throws InterruptedException {
        //things are asynchronous: wait until expected conditions are met (or time out)
        long start = System.currentTimeMillis();
        while (metricsService.getMeter(meterName) == null
                || metricsService.getMeter(meterName).getCount() < expectedCount) {
            if ((System.currentTimeMillis() - start) > 5000) {
                fail("This takes to long...");
            }
            Thread.sleep(10);
        }
    }

    private void wiremock(String classpathResource, String url) throws IOException {
        byte[] bytes = IOUtils.toByteArray(this.getClass().getResourceAsStream(classpathResource));
        stubFor(get(urlEqualTo(url))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/xml")
                        .withBody(bytes)));
    }


}