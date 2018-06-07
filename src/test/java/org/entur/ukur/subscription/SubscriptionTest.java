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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SubscriptionTest {

    @Test
    public void verifyNoExceptions() {
        Subscription s = new Subscription();
        s.setName("Bare tull og tøys");
        assertNull(s.getSiriRequestor());
        assertNull(s.getSiriClientGeneratedId());
    }

    @Test
    public void verifySiriNameNoTroubleExceptions() {
        Subscription s = new Subscription();
        s.setName(Subscription.getName("Requestor", "ClientID"));
        assertEquals("Requestor", s.getSiriRequestor());
        assertEquals("ClientID", s.getSiriClientGeneratedId());
    }

}