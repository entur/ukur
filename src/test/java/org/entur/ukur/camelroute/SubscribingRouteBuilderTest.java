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

import com.hazelcast.core.IMap;
import org.apache.commons.io.IOUtils;
import org.entur.ukur.App;
import org.entur.ukur.camelroute.testconfig.WiremockTestConfig;
import org.entur.ukur.service.MetricsService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
@AutoConfigureWireMock(port = 0)
@TestPropertySource("classpath:application-subscribing.properties")
@DirtiesContext
public class SubscribingRouteBuilderTest extends AbstractJUnit4SpringContextTests {


    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private MetricsService metricsService;

    @Autowired
    private WiremockTestConfig config;

    @Autowired @Qualifier("sharedProperties")
    IMap<String, String> sharedProperties;

    @Test
    public void testETreceive() throws Exception {

        waitUntilReceiverIsReady();

        assertEquals(0, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());

        postFile("/et-pretty.xml", "et");
        waitUntil(MetricsService.TIMER_ET_PROCESS, 1);

        assertEquals(1, metricsService.getMeter("message.received.EstimatedVehicleJourney").getCount());
        assertEquals(1, metricsService.getTimer(MetricsService.TIMER_ET_PROCESS).getCount());
    }

    @Test
    public void testSXreceive() throws Exception {

        waitUntilReceiverIsReady();

        assertEquals(0, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(0, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());

        postFile("/sx-pretty.xml", "sx");
        waitUntil(MetricsService.TIMER_SX_PROCESS, 5);

        assertEquals(5, metricsService.getMeter("message.received.PtSituationElement").getCount());
        assertEquals(5, metricsService.getTimer(MetricsService.TIMER_SX_PROCESS).getCount());
    }

    private void postFile(String classpathResource, String type) throws IOException {
        String requestorId = sharedProperties.get("AnsharRequestorId");
        assertNotNull(requestorId);
        byte[] bytes = IOUtils.toByteArray(this.getClass().getResourceAsStream(classpathResource));
        String postUrl = "http://localhost:"+config.getRestPort()+"/siriMessages/"+requestorId+"/"+type+"/";
        HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/xml");
        connection.setRequestProperty("Content-Length", "" +Integer.toString(bytes.length));
        connection.setDoOutput(true);
        DataOutputStream out = new DataOutputStream(connection.getOutputStream());
        out.write(bytes);
        out.flush();
        out.close();
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode);
    }

    private void waitUntilReceiverIsReady() throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5_000) {
            String postUrl = "http://localhost:" + config.getRestPort() + "/siriMessages/illegal-requestorId/illegal-type/";
            String body = "Will not be read";
            logger.debug("Check if server responds on uri: {}", postUrl);
            HttpURLConnection connection = (HttpURLConnection) new URL(postUrl).openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/xml");
            connection.setRequestProperty("Content-Length", "" +Integer.toString(body.getBytes().length));
            connection.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            out.writeBytes(body);
            out.flush();
            out.close();
            int responseCode = connection.getResponseCode();
            logger.debug("... responseCode = {}", responseCode);
            if (responseCode == 403) {
                logger.info("The server responds FORBIDDEN on illegal input and is ready!");
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for receiver");
    }

    private void waitUntil(String timer, int expectedCount) throws InterruptedException {
        //things are asynchronous: wait until expected conditions are met (or time out)
        long start = System.currentTimeMillis();
        while (metricsService.getTimer(timer).getCount() < expectedCount) {
            if ((System.currentTimeMillis() - start) > 5000) {
                fail("This takes to long...");
            }
            Thread.sleep(10);
        }
    }


}