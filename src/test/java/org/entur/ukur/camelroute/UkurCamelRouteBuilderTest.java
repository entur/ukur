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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.App;
import org.entur.ukur.camelroute.status.SubscriptionStatus;
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
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.PtSituationElement;

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
    private NsbETSubscriptionProcessor nsbETSubscriptionProcessor;

    @Autowired
    private NsbSXSubscriptionProcessor nsbSXSubscriptionProcessor;

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

        ObjectWriter objectWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();
        SubscriptionStatus status = nsbETSubscriptionProcessor.getStatus();
        assertNull(status.getLastProcessed());
        assertTrue(status.getProcessedCounter().isEmpty());
        assertNull(status.getLastHandled());
        assertTrue(status.getHandledCounter().isEmpty());

        logger.debug("\n\n---START---\n{}\n\n", objectWriter.writeValueAsString(status));
        etTemplate.sendBody("go!");
        waitUntil(status, EstimatedVehicleJourney.class, 1);
        logger.debug("\n\n---END---\n{}\n\n", objectWriter.writeValueAsString(status));

        assertNotNull(status.getLastProcessed());
        assertEquals(1, status.getProcessedCounter().size());
        assertEquals(new Long(1), status.getProcessedCounter().get(EstimatedVehicleJourney.class));
        assertNotNull(status.getLastHandled());
        assertEquals(new Long(1), status.getHandledCounter().get(EstimatedVehicleJourney.class));
    }

    @Test
    public void testSXpolling() throws Exception {
        wiremock("/sx-pretty.xml", "/sx");

        ObjectWriter objectWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();
        SubscriptionStatus status = nsbSXSubscriptionProcessor.getStatus();
        assertNull(status.getLastProcessed());
        assertTrue(status.getProcessedCounter().isEmpty());
        assertNull(status.getLastHandled());
        assertTrue(status.getHandledCounter().isEmpty());

        logger.debug("\n\n---START---\n{}\n\n", objectWriter.writeValueAsString(status));
        sxTemplate.sendBody("go!");
        waitUntil(status, PtSituationElement.class, 5);
        logger.debug("\n\n---END---\n{}\n\n", objectWriter.writeValueAsString(status));

        assertNotNull(status.getLastProcessed());
        assertEquals(1, status.getProcessedCounter().size());
        assertEquals(new Long(5), status.getProcessedCounter().get(PtSituationElement.class));
        assertNotNull(status.getLastHandled());
        assertEquals(new Long(5), status.getHandledCounter().get(PtSituationElement.class));
    }

    private void waitUntil(SubscriptionStatus status, Class keyClass, int expectedCount) throws InterruptedException {
        //things are asynchronous: wait until expected conditions are met (or time out)
        long start = System.currentTimeMillis();
        while (status.getHandledCounter().get(keyClass) == null
                || status.getHandledCounter().get(keyClass) < expectedCount) {
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