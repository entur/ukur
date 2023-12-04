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

import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.xml.SiriObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.EstimatedTimetableRequestStructure;
import uk.org.siri.siri20.EstimatedTimetableSubscriptionStructure;
import uk.org.siri.siri20.OtherErrorStructure;
import uk.org.siri.siri20.ResponseStatus;
import uk.org.siri.siri20.ServiceDeliveryErrorConditionElement;
import uk.org.siri.siri20.Siri;
import uk.org.siri.siri20.SituationExchangeRequestStructure;
import uk.org.siri.siri20.SituationExchangeSubscriptionStructure;
import uk.org.siri.siri20.SubscriptionContextStructure;
import uk.org.siri.siri20.SubscriptionRequest;
import uk.org.siri.siri20.SubscriptionResponseStructure;
import uk.org.siri.siri20.TerminateSubscriptionRequestStructure;
import uk.org.siri.siri20.TerminateSubscriptionResponseStructure;
import uk.org.siri.siri20.TerminationResponseStatusStructure;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;

@Service
public class SiriXMLSubscriptionHandler {

    public static final String SIRI_VERSION = "2.0";
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private SubscriptionManager subscriptionManager;

    @Autowired
    public SiriXMLSubscriptionHandler(SubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    @SuppressWarnings({"unused", "UnusedReturnValue", "WeakerAccess"}) //Used from Camel REST api
    public Siri handle(Siri request, String codespace) {
        logger.info("Siri subscription request received with codespace={}", codespace);
        if (request.getSubscriptionRequest() != null) {
            SubscriptionRequest subscriptionRequest = request.getSubscriptionRequest();
            String requestorRef = getStringValue(subscriptionRequest.getRequestorRef());
            String address = subscriptionRequest.getAddress();
            SubscriptionContextStructure subscriptionContext = subscriptionRequest.getSubscriptionContext();
            Duration heartbeatInterval = subscriptionContext != null ? subscriptionContext.getHeartbeatInterval() : null;

            //SX subscription part
            int noSxSubscriptionRequests = subscriptionRequest.getSituationExchangeSubscriptionRequests().size();
            if (noSxSubscriptionRequests > 0) {
                if (noSxSubscriptionRequests == 1) {
                    SituationExchangeSubscriptionStructure sxSubscriptionReq = subscriptionRequest.getSituationExchangeSubscriptionRequests().get(0);
                    String subscriptionIdentifier = getStringValue(sxSubscriptionReq.getSubscriptionIdentifier());
                    ZonedDateTime initialTerminationTime = sxSubscriptionReq.getInitialTerminationTime();
                    SituationExchangeRequestStructure sxRequest = sxSubscriptionReq.getSituationExchangeRequest();
                    Set<String> lineRefs = sxRequest.getLineReves().stream().map(SiriObjectHelper::getStringValue).collect(Collectors.toSet());
                    return addOrReplaceSubscription(requestorRef, subscriptionIdentifier, initialTerminationTime, heartbeatInterval, codespace, lineRefs, address, SubscriptionTypeEnum.SX);

                } else {
                    return generateSubscriptionResponse(false, requestorRef, "Only one SituationExchangeSubscriptionRequest is supported");
                }
            }

            //ET subscription part
            int noEtSubscriptionRequests = subscriptionRequest.getEstimatedTimetableSubscriptionRequests().size();
            if (noEtSubscriptionRequests > 0) {
                if (noEtSubscriptionRequests == 1) {
                    EstimatedTimetableSubscriptionStructure etSubscriptionReq = subscriptionRequest.getEstimatedTimetableSubscriptionRequests().get(0);
                    String subscriptionIdentifier = getStringValue(etSubscriptionReq.getSubscriptionIdentifier());
                    ZonedDateTime initialTerminationTime = etSubscriptionReq.getInitialTerminationTime();
                    EstimatedTimetableRequestStructure etRequest = etSubscriptionReq.getEstimatedTimetableRequest();
                    Set<String> lineRefs = Collections.emptySet();
                    EstimatedTimetableRequestStructure.Lines lines = etRequest.getLines();
                    if (lines != null) {
                        lineRefs = lines.getLineDirections().stream().map(ld -> getStringValue(ld.getLineRef())).collect(Collectors.toSet());
                    }
                    return addOrReplaceSubscription(requestorRef, subscriptionIdentifier, initialTerminationTime, heartbeatInterval, codespace, lineRefs, address, SubscriptionTypeEnum.ET);

                } else {
                    return generateSubscriptionResponse(false, requestorRef, "Only one EstimatedTimetableSubscriptionRequest is supported");
                }
            }

            return generateSubscriptionResponse(false, requestorRef, "Requires either a SituationExchangeSubscriptionRequest or an EstimatedTimetableSubscriptionRequest");
        } else if (request.getTerminateSubscriptionRequest() != null) {
            TerminateSubscriptionRequestStructure terminateSubscriptionRequest = request.getTerminateSubscriptionRequest();
            String requestorRef = getStringValue(terminateSubscriptionRequest.getRequestorRef());
            if (StringUtils.isBlank(requestorRef)) {
                return generateTerminateSubscriptionResponse(false, requestorRef, "RequestorRef is required");
            }
            if (terminateSubscriptionRequest.getSubscriptionReves().size() != 1) {
                return generateTerminateSubscriptionResponse(false, requestorRef, "A single SubscriptionRef is required");
            }
            String subscriptionRef = getStringValue(terminateSubscriptionRequest.getSubscriptionReves().get(0));
            logger.info("New TerminateSubscriptionRequest: requestorRef={}, subscriptionRef={}", requestorRef, subscriptionRef);
            String name = Subscription.getName(requestorRef, subscriptionRef);
            Subscription subscription = subscriptionManager.getSubscriptionByName(name);
            if (subscription != null) {
                subscriptionManager.remove(subscription.getId());
            } else {
                //We respond successfull regardless of the subscription actually exists so we can't be used to guess subscription names
                logger.warn("TerminateSubscriptionRequest on unmatched subscription, generated name is: {}", name);
            }
            return generateTerminateSubscriptionResponse(true, requestorRef, null);
        } else {
            logger.warn("Got an unknown Siri-request");
            //TODO: create new exception class and add exception handler in camel route to give proper errormessages to client
            throw new RuntimeException("Unknown request");
        }
    }

    private Siri addOrReplaceSubscription(String requestorRef,
                                          String subscriptionIdentifier,
                                          ZonedDateTime initialTerminationTime,
                                          Duration heartbeatInterval,
                                          String codespace,
                                          Set<String> lineRefs,
                                          String address,
                                          SubscriptionTypeEnum type){
        logger.info("New {} subscription (siri XML): requestorRef={}, subscriptionIdentifier={}, initialTerminationTime={}," +
                        " heartbeatInterval={}, codespace={}, lines={}, address={}",
                type, requestorRef, subscriptionIdentifier, initialTerminationTime, heartbeatInterval, codespace, lineRefs, address);

        StringBuilder error = new StringBuilder();
        if (StringUtils.isBlank(requestorRef)) {
            error.append("RequestorRef is required.\n");
        }
        if (StringUtils.isBlank(address)) {
            error.append("Address is required.\n");
        }
        if (StringUtils.isBlank(subscriptionIdentifier)) {
            error.append("SubscriptionIdentifier is required.\n");
        }
        if (StringUtils.isBlank(codespace) && lineRefs.isEmpty()) {
            error.append("Either codespace (add desired codespace to the url, ex: '/ABC') or one/more linerefs are required.\n");
        }
        if (initialTerminationTime != null && ZonedDateTime.now().isAfter(initialTerminationTime)) {
            error.append("InitialTerminationTime is in the past.\n");
        }
        if (error.length() > 0) {
            return generateSubscriptionResponse(false, requestorRef, error.toString());
        }

        Subscription subscription = new Subscription();
        subscription.setUseSiriSubscriptionModel(true);
        subscription.setPushAddress(address);
        String name = Subscription.getName(requestorRef, subscriptionIdentifier);
        subscription.setName(name); //(mis-)use name as a second identifier
        subscription.setType(type);
        if (StringUtils.isNotBlank(codespace)) {
            subscription.addCodespace(codespace);
        }
        for (String lineRef : lineRefs) {
            subscription.addLineRef(lineRef);
        }
        if (initialTerminationTime != null) {
            subscription.setInitialTerminationTime(initialTerminationTime);
        }
        if (heartbeatInterval != null) {
            subscription.setHeartbeatInterval(heartbeatInterval);
        }
        Subscription existing = subscriptionManager.getSubscriptionByName(name);
        if (existing != null) {
            subscription.setId(existing.getId());
        }
        subscriptionManager.addOrUpdate(subscription, true);
        return generateSubscriptionResponse(true, requestorRef, null);
    }

    private Siri generateTerminateSubscriptionResponse(boolean success, String requestorRef, String errorMessage) {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        ZonedDateTime now = ZonedDateTime.now();
        TerminateSubscriptionResponseStructure response = new TerminateSubscriptionResponseStructure();
        response.setResponseTimestamp(now);
        TerminationResponseStatusStructure responseStatus = new TerminationResponseStatusStructure();
        responseStatus.setResponseTimestamp(now);
        responseStatus.setStatus(success);
        if (!success) {
            logger.warn("Could not process TerminateSubscriptionRequest (requestorRef={}): {}", requestorRef, errorMessage);
            TerminationResponseStatusStructure.ErrorCondition condition = new TerminationResponseStatusStructure.ErrorCondition();
            OtherErrorStructure otherError = new OtherErrorStructure();
            otherError.setErrorText(errorMessage);
            condition.setOtherError(otherError);
            responseStatus.setErrorCondition(condition);
        } else {
            logger.info("Successfully processed TerminateSubscriptionRequest (requestorRef={})", requestorRef);
        }
        response.getTerminationResponseStatuses().add(responseStatus);
        siri.setTerminateSubscriptionResponse(response);
        return siri;
    }

    private Siri generateSubscriptionResponse(boolean success, String requestorRef, String errorMessage) {
        Siri siri = new Siri();
        siri.setVersion(SIRI_VERSION);
        ZonedDateTime now = ZonedDateTime.now();
        SubscriptionResponseStructure subscriptionResponse = new SubscriptionResponseStructure();
        subscriptionResponse.setResponseTimestamp(now);
        ResponseStatus responseStatus = new ResponseStatus();
        responseStatus.setResponseTimestamp(now);
        responseStatus.setStatus(success);
        if (!success) {
            logger.warn("Could not process SubscriptionRequest (requestorRef={}): {}", requestorRef, errorMessage);
            ServiceDeliveryErrorConditionElement serviceDeliveryErrorConditionElement = new ServiceDeliveryErrorConditionElement();
            OtherErrorStructure otherError = new OtherErrorStructure();
            otherError.setErrorText(errorMessage);
            serviceDeliveryErrorConditionElement.setOtherError(otherError);
            responseStatus.setErrorCondition(serviceDeliveryErrorConditionElement);
        } else {
            logger.info("Successfully processed SubscriptionRequest (requestorRef={})", requestorRef);
        }
        subscriptionResponse.getResponseStatuses().add(responseStatus);
        siri.setSubscriptionResponse(subscriptionResponse);
        return siri;
    }

}
