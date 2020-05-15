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

import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.App;
import org.entur.ukur.service.MetricsService;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
@AutoConfigureWireMock(port = 0)
@TestPropertySource("classpath:application-polling.properties")
@DirtiesContext
@Ignore
public class PollingRouteBuilderTest extends AbstractJUnit4SpringContextTests {

    @Autowired
    private MetricsService metricsService;

    @Produce(uri = UkurCamelRouteBuilder.ROUTE_ET_RETRIEVER)
    private ProducerTemplate etTemplate;

    @Produce(uri = UkurCamelRouteBuilder.ROUTE_SX_RETRIEVER)
    private ProducerTemplate sxTemplate;

    @Test
    public void testETpolling() throws Exception {
        wiremock("/et-pretty.xml", "/et");

        assertEquals(0, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());

        etTemplate.sendBody("go!");
        waitUntil(MetricsService.TIMER_ET_PROCESS, 10);

        assertEquals(10, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(10, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());
    }

    @Test
    public void testSXpolling() throws Exception {
        wiremock("/sx-pretty.xml", "/sx");

        assertEquals(0, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());

        sxTemplate.sendBody("go!");
        waitUntil(MetricsService.TIMER_SX_PROCESS, 9);

        assertEquals(9, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(9, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
    }

    private void waitUntil(String timer, int expectedCount) throws InterruptedException {
        //things are asynchronous: wait until expected conditions are met (or time out)
        long start = System.currentTimeMillis();
        while (metricsService.getTimer(timer).getCount() < expectedCount) {
//            logger.debug("got "+metricsService.getTimer(timer).getCount()+" - expects "+ expectedCount);
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