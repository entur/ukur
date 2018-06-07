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
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import uk.org.siri.siri20.*;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.entur.ukur.subscription.SiriXMLSubscriptionHandler.SIRI_VERSION;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SiriXMLSubscriptionHandlerTest {

    private final static String HEARTBEAT_INTERVAL = "PT5M";
    private final static ZonedDateTime now = ZonedDateTime.now();

    @Mock
    private SubscriptionManager subscriptionManagerMock;
    @InjectMocks
    private SiriXMLSubscriptionHandler siriXMLSubscriptionHandler;

    @Test
    public void testCreateETSubscription() throws Exception {
        //All OK and all optional fields filled out
        Siri request = createSubscriptionRequest(false, true);
        Siri response = siriXMLSubscriptionHandler.handle(request, "ABC");
        assertNotNull(response);
        assertNotNull(response.getSubscriptionResponse());
        assertNotNull(response.getSubscriptionResponse().getResponseStatuses());
        assertEquals(1, response.getSubscriptionResponse().getResponseStatuses().size());
        ResponseStatus status = response.getSubscriptionResponse().getResponseStatuses().get(0);
        assertTrue(status.isStatus());
        assertNull(status.getErrorCondition());
        ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
        ArgumentCaptor<Boolean> booleanCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(subscriptionManagerMock).addOrUpdate(subscriptionCaptor.capture(), booleanCaptor.capture());
        assertTrue(booleanCaptor.getValue());
        Subscription subscription = subscriptionCaptor.getValue();
        assertEquals(Subscription.getName("Requestor", "clientGeneratedSubscriptionId-2"), subscription.getName());
        assertEquals("https://server:port/pushaddress", subscription.getPushAddress());
        assertEquals(SubscriptionTypeEnum.ET, subscription.getType());
        assertTrue(subscription.isSiriXMLBasedSubscription());
        assertTrue(subscription.hasNoStops());
        assertThat(subscription.getLineRefs(), containsInAnyOrder("NSB:Line:L1", "RUT:Line:5"));
        assertThat(subscription.getCodespaces(), containsInAnyOrder("ABC"));
        assertNotNull(subscription.getHeartbeatInterval());
        assertNotNull(subscription.getInitialTerminationTime());
    }

    @Test
    public void testCreateETSubscriptionWithOnlyRequiredFields()  {
        //the timestamps are required by the XSD, not our code...
        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue("Requestor");
        SubscriptionRequest request = new SubscriptionRequest();
        request.setRequestorRef(requestorRef);
        request.setAddress("https://server:port/pushaddress");
        EstimatedTimetableRequestStructure etRequest = new EstimatedTimetableRequestStructure();
        EstimatedTimetableSubscriptionStructure etSubscriptionReq = new EstimatedTimetableSubscriptionStructure();
        etSubscriptionReq.setEstimatedTimetableRequest(etRequest);
        SubscriptionQualifierStructure etSubscriptionIdentifier = new SubscriptionQualifierStructure();
        etSubscriptionIdentifier.setValue("Id");
        etSubscriptionReq.setSubscriptionIdentifier(etSubscriptionIdentifier);
        request.getEstimatedTimetableSubscriptionRequests().add(etSubscriptionReq);
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        siri.setSubscriptionRequest(request);

        Siri response = siriXMLSubscriptionHandler.handle(siri, "ABC");
        assertNotNull(response);
        assertNotNull(response.getSubscriptionResponse());
        assertNotNull(response.getSubscriptionResponse().getResponseStatuses());
        assertEquals(1, response.getSubscriptionResponse().getResponseStatuses().size());
        ResponseStatus status = response.getSubscriptionResponse().getResponseStatuses().get(0);
        assertTrue(status.isStatus());
        assertNull(status.getErrorCondition());
        ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
        ArgumentCaptor<Boolean> booleanCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(subscriptionManagerMock).addOrUpdate(subscriptionCaptor.capture(), booleanCaptor.capture());
        assertTrue(booleanCaptor.getValue());
        Subscription subscription = subscriptionCaptor.getValue();
        assertEquals(Subscription.getName("Requestor", "Id"), subscription.getName());
        assertEquals("https://server:port/pushaddress", subscription.getPushAddress());
        assertEquals(SubscriptionTypeEnum.ET, subscription.getType());
        assertTrue(subscription.isSiriXMLBasedSubscription());
        assertTrue(subscription.hasNoStops());
        assertTrue(subscription.getLineRefs().isEmpty());
        assertThat(subscription.getCodespaces(), containsInAnyOrder("ABC"));
        assertNull(subscription.getHeartbeatInterval());
        assertNull(subscription.getInitialTerminationTime());
    }

    @Test
    public void testCreateETSubscriptionWithoutCriteria() throws Exception {
        //Without line or codespace
        Siri request = createSubscriptionRequest(false, false);
        Siri response = siriXMLSubscriptionHandler.handle(request, null);
        assertNotNull(response);
        assertNotNull(response.getSubscriptionResponse());
        assertNotNull(response.getSubscriptionResponse().getResponseStatuses());
        assertEquals(1, response.getSubscriptionResponse().getResponseStatuses().size());
        ResponseStatus status = response.getSubscriptionResponse().getResponseStatuses().get(0);
        assertFalse(status.isStatus());
        assertNotNull(status.getErrorCondition());
        assertNotNull(status.getErrorCondition().getOtherError());
        assertEquals("Either codespace (add desired codespace to the url, ex: '/ABC') or one/more linerefs are required.\n", status.getErrorCondition().getOtherError().getErrorText());
    }

    @Test
    public void testCreateSXSubscription() throws Exception {
        //All OK and all optional fields filled out
        Siri request = createSubscriptionRequest(true, true);
        Siri response = siriXMLSubscriptionHandler.handle(request, "ABC");
        assertNotNull(response);
        assertNotNull(response.getSubscriptionResponse());
        assertNotNull(response.getSubscriptionResponse().getResponseStatuses());
        assertEquals(1, response.getSubscriptionResponse().getResponseStatuses().size());
        ResponseStatus status = response.getSubscriptionResponse().getResponseStatuses().get(0);
        assertTrue(status.isStatus());
        assertNull(status.getErrorCondition());
        ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
        ArgumentCaptor<Boolean> booleanCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(subscriptionManagerMock).addOrUpdate(subscriptionCaptor.capture(), booleanCaptor.capture());
        assertTrue(booleanCaptor.getValue());
        Subscription subscription = subscriptionCaptor.getValue();
        assertEquals(Subscription.getName("Requestor", "clientGeneratedSubscriptionId-1"), subscription.getName());
        assertEquals("https://server:port/pushaddress", subscription.getPushAddress());
        assertEquals(SubscriptionTypeEnum.SX, subscription.getType());
        assertTrue(subscription.isSiriXMLBasedSubscription());
        assertTrue(subscription.hasNoStops());
        assertThat(subscription.getLineRefs(), containsInAnyOrder("NSB:Line:L1", "RUT:Line:5"));
        assertThat(subscription.getCodespaces(), containsInAnyOrder("ABC"));
        assertNotNull(subscription.getHeartbeatInterval());
        assertNotNull(subscription.getInitialTerminationTime());
    }

    @Test
    public void testCreateSXSubscriptionWithOnlyRequiredFields()  {
        //the timestamps are required by the XSD, not our code...
        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue("Req");
        SubscriptionRequest request = new SubscriptionRequest();
        request.setRequestorRef(requestorRef);
        request.setAddress("pushaddress");
        SituationExchangeRequestStructure sxRequest = new SituationExchangeRequestStructure();
        SituationExchangeSubscriptionStructure sxSubscriptionReq = new SituationExchangeSubscriptionStructure();
        sxSubscriptionReq.setSituationExchangeRequest(sxRequest);
        SubscriptionQualifierStructure sxSubscriptionIdentifier = new SubscriptionQualifierStructure();
        sxSubscriptionIdentifier.setValue("Id");
        sxSubscriptionReq.setSubscriptionIdentifier(sxSubscriptionIdentifier);
        request.getSituationExchangeSubscriptionRequests().add(sxSubscriptionReq);
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        siri.setSubscriptionRequest(request);

        Siri response = siriXMLSubscriptionHandler.handle(siri, "Test");
        assertNotNull(response);
        assertNotNull(response.getSubscriptionResponse());
        assertNotNull(response.getSubscriptionResponse().getResponseStatuses());
        assertEquals(1, response.getSubscriptionResponse().getResponseStatuses().size());
        ResponseStatus status = response.getSubscriptionResponse().getResponseStatuses().get(0);
        assertTrue(status.isStatus());
        assertNull(status.getErrorCondition());
        ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
        ArgumentCaptor<Boolean> booleanCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(subscriptionManagerMock).addOrUpdate(subscriptionCaptor.capture(), booleanCaptor.capture());
        assertTrue(booleanCaptor.getValue());
        Subscription subscription = subscriptionCaptor.getValue();
        assertEquals(Subscription.getName("Req", "Id"), subscription.getName());
        assertEquals("pushaddress", subscription.getPushAddress());
        assertEquals(SubscriptionTypeEnum.SX, subscription.getType());
        assertTrue(subscription.isSiriXMLBasedSubscription());
        assertTrue(subscription.hasNoStops());
        assertTrue(subscription.getLineRefs().isEmpty());
        assertThat(subscription.getCodespaces(), containsInAnyOrder("Test"));
        assertNull(subscription.getHeartbeatInterval());
        assertNull(subscription.getInitialTerminationTime());
    }

    @Test
    public void testCreateSXSubscriptionWithoutCriteria() throws Exception {
        //Without line or codespace
        Siri request = createSubscriptionRequest(true, false);
        Siri response = siriXMLSubscriptionHandler.handle(request, null);
        assertNotNull(response);
        assertNotNull(response.getSubscriptionResponse());
        assertNotNull(response.getSubscriptionResponse().getResponseStatuses());
        assertEquals(1, response.getSubscriptionResponse().getResponseStatuses().size());
        ResponseStatus status = response.getSubscriptionResponse().getResponseStatuses().get(0);
        assertFalse(status.isStatus());
        assertNotNull(status.getErrorCondition());
        assertNotNull(status.getErrorCondition().getOtherError());
        assertEquals("Either codespace (add desired codespace to the url, ex: '/ABC') or one/more linerefs are required.\n", status.getErrorCondition().getOtherError().getErrorText());
    }

    @Test
    public void testTerminateSubscription() {
        //all OK (response does not differ if subscription exists or not)
        Siri request = terminateSubscriptionRequest();
        Siri terminateSubscriptionResponse = siriXMLSubscriptionHandler.handle(request, null);
        assertNotNull(terminateSubscriptionResponse);
        assertNotNull(terminateSubscriptionResponse.getTerminateSubscriptionResponse());
        assertNotNull(terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses());
        assertEquals(1, terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses().size());
        TerminationResponseStatusStructure status = terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses().get(0);
        assertTrue(status.isStatus());
        assertNull(status.getErrorCondition());

        //misses RequestorRef
        request = terminateSubscriptionRequest();
        request.getTerminateSubscriptionRequest().setRequestorRef(null);
        terminateSubscriptionResponse = siriXMLSubscriptionHandler.handle(request, null);
        assertNotNull(terminateSubscriptionResponse);
        assertNotNull(terminateSubscriptionResponse.getTerminateSubscriptionResponse());
        assertNotNull(terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses());
        assertEquals(1, terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses().size());
        status = terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses().get(0);
        assertFalse(status.isStatus());
        assertNotNull(status.getErrorCondition());
        assertNotNull(status.getErrorCondition().getOtherError());
        assertEquals("RequestorRef is required", status.getErrorCondition().getOtherError().getErrorText());

        //misses SubscriptionReves
        request = terminateSubscriptionRequest();
        request.getTerminateSubscriptionRequest().getSubscriptionReves().clear();
        terminateSubscriptionResponse = siriXMLSubscriptionHandler.handle(request, null);
        assertNotNull(terminateSubscriptionResponse);
        assertNotNull(terminateSubscriptionResponse.getTerminateSubscriptionResponse());
        assertNotNull(terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses());
        assertEquals(1, terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses().size());
        status = terminateSubscriptionResponse.getTerminateSubscriptionResponse().getTerminationResponseStatuses().get(0);
        assertFalse(status.isStatus());
        assertNotNull(status.getErrorCondition());
        assertNotNull(status.getErrorCondition().getOtherError());
        assertEquals("A single SubscriptionRef is required", status.getErrorCondition().getOtherError().getErrorText());

    }


    static Siri terminateSubscriptionRequest() {
        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue("Requestor");
        TerminateSubscriptionRequestStructure terminateSubscriptionRequestStructure = new TerminateSubscriptionRequestStructure();
        terminateSubscriptionRequestStructure.setRequestorRef(requestorRef);
        terminateSubscriptionRequestStructure.setRequestTimestamp(now);
        SubscriptionQualifierStructure subscriptionQualifierStructure = new SubscriptionQualifierStructure();
        subscriptionQualifierStructure.setValue("clientGeneratedSubscriptionId");
        terminateSubscriptionRequestStructure.getSubscriptionReves().add(subscriptionQualifierStructure);

        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        siri.setTerminateSubscriptionRequest(terminateSubscriptionRequestStructure);

        return siri;
    }

    static Siri createSubscriptionRequest(boolean sx, boolean addLines) throws Exception {
        DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
        Duration heartbeatInterval = datatypeFactory.newDuration(HEARTBEAT_INTERVAL);
        LineRef lineRef1 = new LineRef();
        lineRef1.setValue("NSB:Line:L1");
        LineRef lineRef2 = new LineRef();
        lineRef2.setValue("RUT:Line:5");

        //Generic part
        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue("Requestor");
        SubscriptionRequest request = new SubscriptionRequest();
        request.setRequestorRef(requestorRef);
        request.setRequestTimestamp(now);
        request.setAddress("https://server:port/pushaddress");
        SubscriptionContextStructure ctx = new SubscriptionContextStructure();

        ctx.setHeartbeatInterval(heartbeatInterval);
        request.setSubscriptionContext(ctx);

        //The XSD allows only one of these...!
        if (sx) {
            //SX subscription part
            SituationExchangeRequestStructure sxRequest = new SituationExchangeRequestStructure();
            sxRequest.setRequestTimestamp(now);
            if (addLines) {
                sxRequest.getLineReves().add(lineRef1);
                sxRequest.getLineReves().add(lineRef2);
            }
            SituationExchangeSubscriptionStructure sxSubscriptionReq = new SituationExchangeSubscriptionStructure();
            sxSubscriptionReq.setSituationExchangeRequest(sxRequest);
            SubscriptionQualifierStructure sxSubscriptionIdentifier = new SubscriptionQualifierStructure();
            sxSubscriptionIdentifier.setValue("clientGeneratedSubscriptionId-1");
            sxSubscriptionReq.setSubscriptionIdentifier(sxSubscriptionIdentifier);
            sxSubscriptionReq.setInitialTerminationTime(ZonedDateTime.of(9999, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()));
            request.getSituationExchangeSubscriptionRequests().add(sxSubscriptionReq);
        } else {
            //ET subscription part
            EstimatedTimetableRequestStructure etRequest = new EstimatedTimetableRequestStructure();
            etRequest.setRequestTimestamp(now);
            if (addLines) {
                EstimatedTimetableRequestStructure.Lines lines = new EstimatedTimetableRequestStructure.Lines();
                LineDirectionStructure line1 = new LineDirectionStructure();
                line1.setLineRef(lineRef1);
                lines.getLineDirections().add(line1);
                LineDirectionStructure line2 = new LineDirectionStructure();
                line2.setLineRef(lineRef2);
                lines.getLineDirections().add(line2);
                etRequest.setLines(lines);
            }
            EstimatedTimetableSubscriptionStructure etSubscriptionReq = new EstimatedTimetableSubscriptionStructure();
            etSubscriptionReq.setEstimatedTimetableRequest(etRequest);
            SubscriptionQualifierStructure etSubscriptionIdentifier = new SubscriptionQualifierStructure();
            etSubscriptionIdentifier.setValue("clientGeneratedSubscriptionId-2");
            etSubscriptionReq.setSubscriptionIdentifier(etSubscriptionIdentifier);
            etSubscriptionReq.setInitialTerminationTime(ZonedDateTime.of(9999, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()));
            request.getEstimatedTimetableSubscriptionRequests().add(etSubscriptionReq);
        }

        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        siri.setSubscriptionRequest(request);

        return siri;
    }


}