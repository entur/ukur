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

import uk.org.siri.siri20.*;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.entur.ukur.subscription.SiriXMLSubscriptionHandler.SIRI_VERSION;

@SuppressWarnings("Duplicates")
class SiriXMLSubscriptionSamples {

    private static Schema schema;
    private static ZonedDateTime now = ZonedDateTime.now();

    public static void main(String[] args) throws Exception {
        schema = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
                .newSchema(new File("/opt/data/entur/siri-java-model/src/main/resources/siri-2.0/xsd/siri.xsd"));

        print("Create SX Subscription Request (without line-filter)", SiriXMLSubscriptionHandlerTest.createSubscriptionRequest(true, false));
        print("Create SX Subscription Request (with line-filter)", SiriXMLSubscriptionHandlerTest.createSubscriptionRequest(true, true));
        print("Create ET Subscription Request (without line-filter)", SiriXMLSubscriptionHandlerTest.createSubscriptionRequest(false, false));
        print("Create ET Subscription Request (with line-filter)", SiriXMLSubscriptionHandlerTest.createSubscriptionRequest(false, true));
        print("Create Subscription Response (OK)", createSubscriptionResponse(true));
        print("Create Subscription Response (ERROR)", createSubscriptionResponse(false));
        print("Heartbeat Notification", heartbeat());
        print("Terminate Subscription Request", SiriXMLSubscriptionHandlerTest.terminateSubscriptionRequest());
        print("Terminate Subscription Response (OK)", terminateSubscriptionResponse(true));
        print("Terminate Subscription Response (ERROR)", terminateSubscriptionResponse(false));
        print("Subscription Terminated Notification", subscriptionTerminatedNotification());
    }

    private static Siri terminateSubscriptionResponse(boolean success) {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        ZonedDateTime now = ZonedDateTime.now();
        TerminateSubscriptionResponseStructure response = new TerminateSubscriptionResponseStructure();
        response.setResponseTimestamp(now);
        TerminationResponseStatusStructure responseStatus = new TerminationResponseStatusStructure();
        responseStatus.setResponseTimestamp(now);
        responseStatus.setStatus(success);
        if (!success) {
            TerminationResponseStatusStructure.ErrorCondition condition = new TerminationResponseStatusStructure.ErrorCondition();
            OtherErrorStructure otherError = new OtherErrorStructure();
            otherError.setErrorText("Could not delete subscription with some textual description of what went wrong");
            condition.setOtherError(otherError);
            responseStatus.setErrorCondition(condition);
        }
        response.getTerminationResponseStatuses().add(responseStatus);
        siri.setTerminateSubscriptionResponse(response);
        return siri;
    }

    private static Siri subscriptionTerminatedNotification() {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);

        SubscriptionTerminatedNotificationStructure subscriptionTerminatedNotification = new SubscriptionTerminatedNotificationStructure();
        subscriptionTerminatedNotification.setResponseTimestamp(now);
        siri.setSubscriptionTerminatedNotification(subscriptionTerminatedNotification);

        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue("Requestor");
        subscriptionTerminatedNotification.getSubscriberRevesAndSubscriptionRevesAndSubscriptionFilterReves().add(requestorRef);

        SubscriptionQualifierStructure subscriptionQualifierStructure = new SubscriptionQualifierStructure();
        subscriptionQualifierStructure.setValue("clientGeneratedSubscriptionId");
        subscriptionTerminatedNotification.getSubscriberRevesAndSubscriptionRevesAndSubscriptionFilterReves().add(subscriptionQualifierStructure);

        return siri;
    }

    private static Siri heartbeat() {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        HeartbeatNotificationStructure heartbeatNotification = new HeartbeatNotificationStructure();
        siri.setHeartbeatNotification(heartbeatNotification);
        heartbeatNotification.setRequestTimestamp(now);
        return siri;
    }


    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void print(String title, Siri siri) throws Exception {
        System.out.println(title);
        char[] spacer = new char[title.length()];
        Arrays.fill(spacer, '*');
        System.out.println(spacer);
        System.out.println(toString(siri, false));
        toString(siri, true); //validates after we print to easier see what is wrong
        System.out.println();
    }

    private static Siri createSubscriptionResponse(boolean success) {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        SubscriptionResponseStructure subscriptionResponse = new SubscriptionResponseStructure();
        subscriptionResponse.setResponseTimestamp(now);
        ResponseStatus responseStatus = new ResponseStatus();
        responseStatus.setResponseTimestamp(now);
        responseStatus.setStatus(success);
        if (!success) {
            ServiceDeliveryErrorConditionElement serviceDeliveryErrorConditionElement = new ServiceDeliveryErrorConditionElement();
            OtherErrorStructure otherError = new OtherErrorStructure();
            otherError.setErrorText("Could not create subscription with some textual description of what went wrong");
            serviceDeliveryErrorConditionElement.setOtherError(otherError);
            responseStatus.setErrorCondition(serviceDeliveryErrorConditionElement);
        }
        subscriptionResponse.getResponseStatuses().add(responseStatus);
        siri.setSubscriptionResponse(subscriptionResponse);
        return siri;
    }

    private static String toString(Object siriElement, boolean validate) throws Exception {
        JAXBContext jaxbContext = JAXBContext.newInstance(Siri.class);
        Marshaller marshaller = jaxbContext.createMarshaller();
        if (validate) {
            marshaller.setSchema(schema);
        }
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
        StringWriter writer = new StringWriter();
        marshaller.marshal(siriElement, writer);
        String xml = writer.getBuffer().toString();

        //removes unused namespace declarations in the first line heading:
        int start = xml.indexOf(" xmlns:");
        int end = xml.indexOf(">");
        xml =  xml.substring(0, start) + xml.substring(end);

        if (validate) {
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            Siri siri = (Siri) jaxbUnmarshaller.unmarshal(new StringReader(xml));
            if (!xml.equals(toString(siri, false))) {
                throw new RuntimeException("The XML is not the same after string->siri-object->string conversions");
            }
        }
        return xml;
    }

}