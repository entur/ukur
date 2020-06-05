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

import java.time.ZonedDateTime;

import static org.junit.Assert.*;

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

    @Test
    public void verifyRemoveLogic() {
        Subscription s = new Subscription();
        assertNull(s.getFirstErrorSeen());
        assertEquals(0, s.getFailedPushCounter());
        for (int i = 0; i < 10; i++) {
            assertFalse(s.shouldRemove());
        }
        assertEquals(10, s.getFailedPushCounter());
        s.setFirstErrorSeen(ZonedDateTime.now().minusMinutes(9).minusSeconds(59));
        assertFalse(s.shouldRemove()); //should not remove as the first error is seen less than 10 minutes ago...

        assertEquals(11, s.getFailedPushCounter());
        s.setFirstErrorSeen(ZonedDateTime.now().minusMinutes(10).minusSeconds(1));
        assertTrue(s.shouldRemove()); //should remove as the first error is seen more than 10 minutes ago and there are more than 3 errors...

        s.resetFailedPushCounter();
        assertNull(s.getFirstErrorSeen());
        assertEquals(0, s.getFailedPushCounter());

        assertFalse(s.shouldRemove());
        s.setFirstErrorSeen(ZonedDateTime.now().minusMinutes(15));
        for (int i = 1; i < 3; i++) {
            assertEquals(i, s.getFailedPushCounter());
            assertFalse(s.shouldRemove());
        }
        assertTrue(s.shouldRemove()); //as the first error is seen 15 minutes ago, we only check that the failedPushCounter is greater than 3
        assertEquals(4, s.getFailedPushCounter());
    }

    @Test
    public void verifyURLParsing() {
        Subscription s = new Subscription();
        s.setPushAddress("https://www.ruter.no/gi-meg-siri-data");
        assertEquals("https://www.ruter.no/gi-meg-siri-data", s.getPushAddress());
        assertEquals("www.ruter.no", s.getPushHost());
    }
}