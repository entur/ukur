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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTesting {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void subscriptionAsJson() throws JsonProcessingException {
        Subscription subscription = new Subscription();
        subscription.setType(SubscriptionTypeEnum.ET);
        subscription.addCodespace("RUT");
        subscription.addCodespace("NSB");
        subscription.addLineRef("RUT:Line:1");
        subscription.addLineRef("NSB:Line:L1");
        subscription.addFromStopPoint("NRS:StopPlace:1");
        subscription.addToStopPoint("NRS:StopPlace:2");
        subscription.setPushAddress("http://localhost:888/blabla");
        ObjectMapper mapper = new ObjectMapper();
        logger.info("JSON: \n{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(subscription));
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
}
