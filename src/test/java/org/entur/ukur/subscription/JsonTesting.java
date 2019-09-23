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

package org.entur.ukur.subscription;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.io.IOException;
import java.time.ZonedDateTime;

import static org.junit.Assert.*;

public class JsonTesting {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void subscriptionAsJson() throws Exception {
        DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();

        Subscription subscription = new Subscription();
        subscription.setType(SubscriptionTypeEnum.ET);
        subscription.addCodespace("RUT");
        subscription.addCodespace("NSB");
        subscription.addLineRef("RUT:Line:1");
        subscription.addLineRef("NSB:Line:L1");
        subscription.addFromStopPoint("NRS:StopPlace:1");
        subscription.addToStopPoint("NRS:StopPlace:2");
        subscription.setPushAddress("http://localhost:888/blabla");
        subscription.setHeartbeatInterval(datatypeFactory.newDuration("PT15M"));
        subscription.setMaxArrivalDelay(datatypeFactory.newDuration("PT30M"));
        subscription.setInitialTerminationTime(ZonedDateTime.now().plusWeeks(1));

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule()); //Camel fixes this for us in production

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(subscription);
        logger.info("JSON: \n{}", json);

        Subscription readSubscription = mapper.readValue(json, Subscription.class);
        assertEquals(subscription.getPushAddress(), readSubscription.getPushAddress());
        assertEquals(subscription.getHeartbeatInterval(), readSubscription.getHeartbeatInterval());
        var expectedInitialTerminationTime = subscription.getInitialTerminationTime().withNano(0);
        var actualInitialTerminationTime = readSubscription.getInitialTerminationTime().withNano(0);
        assertTrue(expectedInitialTerminationTime.isEqual(actualInitialTerminationTime));
        assertEquals(subscription.getType(), readSubscription.getType());
       assertEquals(subscription.getMaxArrivalDelay(),readSubscription.getMaxArrivalDelay());
    }

    @Test
    public void qarutSubscriptionAsJson() throws JsonProcessingException {
        Subscription subscription = new Subscription();
        subscription.setType(SubscriptionTypeEnum.SX);
        subscription.addCodespace("QA-RUT");
        subscription.setPushAddress("http://public-ruter-server/push");
        ObjectMapper mapper = new ObjectMapper();
        logger.info("JSON: \n{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(subscription));
    }

    @Test
    public void testVariousJson() throws IOException, DatatypeConfigurationException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule()); //Camel fixes this for us in production
        Subscription emptySubscription = mapper.readValue("{}", Subscription.class);
        assertNotNull(emptySubscription);
        assertNull(emptySubscription.getId());
        assertNotNull(emptySubscription.getName());
        assertNull(emptySubscription.getPushAddress());
        assertFalse(emptySubscription.isPushAllData());
        assertNotNull(emptySubscription.getFromStopPoints());
        assertTrue(emptySubscription.getFromStopPoints().isEmpty());
        assertNotNull(emptySubscription.getToStopPoints());
        assertTrue(emptySubscription.getToStopPoints().isEmpty());
        assertNotNull(emptySubscription.getLineRefs());
        assertTrue(emptySubscription.getLineRefs().isEmpty());
        assertNotNull(emptySubscription.getCodespaces());
        assertTrue(emptySubscription.getCodespaces().isEmpty());
        assertEquals(SubscriptionTypeEnum.ALL, emptySubscription.getType());
        assertFalse(emptySubscription.isUseSiriSubscriptionModel());
        assertNull(emptySubscription.getInitialTerminationTime());
        assertNull(emptySubscription.getHeartbeatInterval());

        String json = "{\n" +
                "\"id\" : \"someId\",\n" +
                "\"name\" : \"blabla\",\n" +
                "\"pushAddress\" : \"http://pushserver/push\",\n" +
                "\"pushAllData\" : \"true\",\n" +
                "\"type\" : \"SX\",\n" +
                "\"useSiriSubscriptionModel\" : \"true\",\n" +
                "\"initialTerminationTime\" : \"2018-08-16T12:49:18.105+0200\"," +
                "\"heartbeatInterval\" : \"PT15M\",\n" +
                "\"maxDelay\" : \"PT30M\"\n" +
                "}";
        Subscription subscription = mapper.readValue(json, Subscription.class);
        assertNotNull(subscription);
        assertEquals("someId", subscription.getId());
        assertEquals("blabla", subscription.getName());
        assertEquals("http://pushserver/push", subscription.getPushAddress());
        assertTrue(subscription.isPushAllData());
        assertEquals(SubscriptionTypeEnum.SX, subscription.getType());
        assertTrue(subscription.isUseSiriSubscriptionModel());
        assertNotNull(subscription.getInitialTerminationTime());
        DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
        assertEquals(datatypeFactory.newDuration("PT15M"), subscription.getHeartbeatInterval());
        assertEquals(datatypeFactory.newDuration("PT30M"), subscription.getMaxArrivalDelay());
    }
}
