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

import org.entur.ukur.routedata.LiveRouteService;
import org.entur.ukur.subscription.Subscription;
import org.entur.ukur.subscription.SubscriptionManager;
import org.entur.ukur.xml.SiriMarshaller;
import org.junit.Test;
import uk.org.siri.siri20.EstimatedCall;
import uk.org.siri.siri20.EstimatedVehicleJourney;
import uk.org.siri.siri20.RecordedCall;
import uk.org.siri.siri20.StopPointRef;

import javax.xml.bind.JAXBException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public class NsbETSubscriptionProcessorTest {


    @Test
    public void validDirection() throws JAXBException {

        EstimatedVehicleJourney.RecordedCalls recordedCalls = new EstimatedVehicleJourney.RecordedCalls();
        addRecordedCall(recordedCalls, "R1", ZonedDateTime.now().minus(2, ChronoUnit.HOURS));
        addRecordedCall(recordedCalls, "R2", ZonedDateTime.now().minus(1, ChronoUnit.HOURS));
        EstimatedVehicleJourney.EstimatedCalls estimatedCalls = new EstimatedVehicleJourney.EstimatedCalls();
        addEstimatedCall(estimatedCalls, "E1", ZonedDateTime.now().plus(1, ChronoUnit.HOURS));
        addEstimatedCall(estimatedCalls, "E2", ZonedDateTime.now().plus(2, ChronoUnit.HOURS));
        EstimatedVehicleJourney journey = new EstimatedVehicleJourney();
        journey.setRecordedCalls(recordedCalls);
        journey.setEstimatedCalls(estimatedCalls);

        NsbETSubscriptionProcessor processor = new NsbETSubscriptionProcessor(mock(SubscriptionManager.class),
                new SiriMarshaller(), mock(LiveRouteService.class), mock(FileStorageService.class));

        //No errors if no hits...
        assertFalse(processor.validDirection(new Subscription(), journey));

        //Only to in journey
        assertFalse(processor.validDirection(createSubscription("X", "E2"), journey));

        //Only from in journey
        assertFalse(processor.validDirection(createSubscription("E2", "X"), journey));

        //To and from in correct order in estimated calls
        assertTrue(processor.validDirection(createSubscription("E1", "E2"), journey));

        //To and from in opposite order in estimated calls
        assertFalse(processor.validDirection(createSubscription("E2", "E1"), journey));

        //correct order: to in estimated calls, from in recorded calls
        assertTrue(processor.validDirection(createSubscription("R1", "E2"), journey));

        //opposite order: to in estimated calls, from in recorded calls
        assertFalse(processor.validDirection(createSubscription("E1", "R1"), journey));

    }

    private Subscription createSubscription(String from, String to) {
        Subscription subscription = new Subscription();
        if (from != null) {
            subscription.addFromStopPoint(from);
        }
        if (to != null) {
            subscription.addToStopPoint(to);
        }
        return subscription;
    }

    private void addEstimatedCall(EstimatedVehicleJourney.EstimatedCalls estimatedCalls, String stopPointRef, ZonedDateTime departureTime) {
        EstimatedCall estimatedCall = new EstimatedCall();
        StopPointRef ref = new StopPointRef();
        ref.setValue(stopPointRef);
        estimatedCall.setStopPointRef(ref);
        estimatedCall.setAimedDepartureTime(departureTime);
        estimatedCalls.getEstimatedCalls().add(estimatedCall);
    }

    private void addRecordedCall(EstimatedVehicleJourney.RecordedCalls recordedCalls, String stopPointRef, ZonedDateTime departureTime) {
        RecordedCall recordedCall = new RecordedCall();
        StopPointRef ref = new StopPointRef();
        ref.setValue(stopPointRef);
        recordedCall.setStopPointRef(ref);
        recordedCall.setAimedDepartureTime(departureTime);
        recordedCalls.getRecordedCalls().add(recordedCall);
    }
}