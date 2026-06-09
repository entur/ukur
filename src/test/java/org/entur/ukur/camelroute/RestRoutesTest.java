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

import org.entur.ukur.App;
import org.entur.ukur.camelroute.testconfig.WiremockTestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.MOCK, classes = App.class)
@TestPropertySource("classpath:application-polling.properties")
@DirtiesContext
public class RestRoutesTest {

    @Autowired
    private WiremockTestConfig config;

    @Autowired
    private StopPlaceQuaysProcessor stopPlaceQuaysProcessor;

    /**
     * Simply call the various internal REST endpoints to make sure they do not have any runtime errors.
     */
    @Test
    public void testInternalRESTendpoints() {
        String baseUrl = "http://localhost:"+config.getRestPort()+"/internal";

        stopPlaceQuaysProcessor.hasRun = true; //to allow health/ready respond OK
        assert200Response(baseUrl + "/health/ready");
        assert200Response(baseUrl + "/health/live");
        assert200Response(baseUrl + "/health/subscriptions");
        assert200Response(baseUrl + "/health/subscriptions/reload");
        assert200Response(baseUrl + "/health/routes");
        assert200Response(baseUrl + "/health/scrape");
    }

    private void assert200Response(String path)  {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(path).openConnection();
            int responseCode = connection.getResponseCode();
            assertEquals(200, responseCode, "Path '"+path+"' did not respond 200");
        } catch (IOException e) {
            fail("Got an exception while calling path '"+path+"': "+e.getClass()+": "+e.getMessage());
        }
    }



}