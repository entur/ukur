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

import com.hazelcast.core.IMap;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.entur.ukur.subscription.PushAcknowledge.FORGET_ME;
import static org.entur.ukur.subscription.PushAcknowledge.OK;

@Service
public class SubscriptionManager {

    private IMap<String, Set<String>> subscriptionsPerStopPoint;
    private IMap<String, Subscription> subscriptions;
    private IMap<String, List<PushMessage>> pushMessagesMemoryStore;
    private IMap<String, Long> alreadySentCache;
    private final SiriMarshaller siriMarshaller;

    private String hostname;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    public SubscriptionManager(IMap<String, Set<String>> subscriptionsPerStopPoint,
                               IMap<String, Subscription> subscriptions,
                               IMap<String, List<PushMessage>> pushMessagesMemoryStore,
                               IMap<String, Long> alreadySentCache,
                               SiriMarshaller siriMarshaller) {
        this.subscriptionsPerStopPoint = subscriptionsPerStopPoint;
        this.subscriptions = subscriptions;
        this.pushMessagesMemoryStore = pushMessagesMemoryStore;
        this.alreadySentCache = alreadySentCache;
        this.siriMarshaller = siriMarshaller;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            logger.info("This nodes hostname is '{}'", hostname);
        } catch (UnknownHostException e) {
            hostname = "random_"+new Random().nextInt(10000); //want to separate message producing nodes from each other, this will work as fallback
            logger.error("Cant resolve hostname - use random name '{}' instead to differentiate nodes", hostname, e);
        }
    }

    @SuppressWarnings("unused") //Used from camel route
    public Collection<Subscription> listAll() {
        Collection<Subscription> all = Collections.unmodifiableCollection(this.subscriptions.values());
        logger.debug("There are {} subscriptions regarding {} unique stoppoints", all.size(), subscriptionsPerStopPoint.keySet().size());
        return all;
    }

    @SuppressWarnings("unused") //Used from camel route
    public List<PushMessage> getData(String id) {
        List<PushMessage> remove = pushMessagesMemoryStore.get(id);
        int size = remove == null ? 0 : remove.size();
        logger.debug("Retrieves {} messages and removes data stored in memory for subscription with id {}", size, id);
        pushMessagesMemoryStore.put(id, new ArrayList<>());
        return remove;
    }

    public void addSubscription(Subscription subscription) {
        if (subscription == null || StringUtils.isBlank(subscription.getId())
                || subscription.getToStopPoints().isEmpty() || subscription.getFromStopPoints().isEmpty()) {
            throw new IllegalArgumentException("Illegal subscription");
        }
        String id = subscription.getId().trim();
        if (subscriptions.containsKey(id)) {
            throw new IllegalArgumentException("Subscription with id='"+id+"' already exists");
        }
        subscription.setId(id);
        subscriptions.put(id, subscription);
        HashSet<String> subscribedStops = new HashSet<>();
        subscribedStops.addAll(subscription.getFromStopPoints());
        subscribedStops.addAll(subscription.getToStopPoints());
        for (String stoppoint : subscribedStops) {
            Set<String> subscriptions = subscriptionsPerStopPoint.get(stoppoint);
            if (subscriptions == null) {
                subscriptions = new HashSet<>();
            }
            subscriptions.add(subscription.getId());
            subscriptionsPerStopPoint.put(stoppoint, subscriptions);//cause of hazelcast
        }
        Set<String> stopRefs = subscriptionsPerStopPoint.keySet();
        logger.debug("There are now subscriptions regarding {} unique stoppoints", stopRefs.size());
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef) {
        Set<String> subscriptionIds = subscriptionsPerStopPoint.get(stopPointRef);
        if (subscriptionIds == null) {
            return Collections.emptySet();
        }
        HashSet<Subscription> result = new HashSet<>(subscriptionIds.size());
        for (String subscriptionId : subscriptionIds) {
            Subscription subscription = subscriptions.get(subscriptionId);
            if (subscription != null) {
                result.add(subscription);
            }
        }
        return result;
    }

    public void notify(HashSet<Subscription> subscriptions, EstimatedCall estimatedCall, EstimatedVehicleJourney estimatedVehicleJourney) {
        String pushMessageName = getPushMessageName(estimatedCall, estimatedVehicleJourney);
        EstimatedVehicleJourney clone = clone(estimatedVehicleJourney);
        EstimatedVehicleJourney.EstimatedCalls ec = new EstimatedVehicleJourney.EstimatedCalls();
        ec.getEstimatedCalls().add(estimatedCall);
        clone.setEstimatedCalls(ec);            //TODO: Fjerner alle andre EstimatedCalls enn gjeldende for å begrense størrelse (mulig det er dumt...?)
        clone.setRecordedCalls(null);           //TODO: Fjerner RecordedCalls for å begrense størrelse (mulig det er dumt...?)
        clone.setIsCompleteStopSequence(null);  //TODO: Fjerner evt IsCompleteStopSequence så det ikke blir feil ihht spec
        pushMessage(subscriptions, toXMLString(clone), pushMessageName);
    }

    public void notify(HashSet<Subscription> subscriptions, PtSituationElement ptSituationElement) {
        ptSituationElement = clone(ptSituationElement);
        ptSituationElement.setAffects(null);//TODO: Fjerner affects for å begrense størrelse (det er litt dumt - mister bla hvilke ruter som er berørt...)
                                            //TODO: Kanskje vi bare skal fjerne affects som ikke inneholder til-fra stoppoint? Eller fjerne affected journey's uten interessante stop?
        String xml = toXMLString(ptSituationElement);
        String pushMessageName = getPushMessageName(ptSituationElement);
        pushMessage(subscriptions, xml, pushMessageName);
    }


    @SuppressWarnings({"unused", "UnusedReturnValue"}) //Used from Camel REST api
    public Subscription add(Subscription s) {
        if (s == null) {
            throw new IllegalArgumentException("No subscription given");
        }
        String id = UUID.randomUUID().toString();
        logger.info("Adds new subscription - assigns id: {}", id);
        s.setId(id);
        //TODO: validation...?
        //TODO: Test push address before add
//        if (StringUtils.isBlank(s.getPushAddress())) {
//            throw new IllegalArgumentException("PushAddress is required");
//        }
        addSubscription(s);
        return s;
    }

    @SuppressWarnings({"unused", "UnusedReturnValue", "WeakerAccess"}) //Used from Camel REST api
    public Subscription remove(String subscriptionId) {
        logger.info("Removes subscription with id {}", subscriptionId);
        Subscription removed = subscriptions.remove(subscriptionId);
        if (removed != null) {
            HashSet<String> subscribedStops = new HashSet<>();
            subscribedStops.addAll(removed.getFromStopPoints());
            subscribedStops.addAll(removed.getToStopPoints());
            logger.debug("Also removes the subscription from these stops: {}", subscribedStops);
            for (String stoppoint : subscribedStops) {
                Set<String> subscriptions = subscriptionsPerStopPoint.get(stoppoint);
                if (subscriptions != null) {
                    subscriptions.remove(removed.getId());
                    subscriptionsPerStopPoint.put(stoppoint, subscriptions); //cause of hazelcast
                }
            }
        }
        return removed;
    }


    private <T extends Serializable> T clone(T toClone) {
        return SerializationUtils.clone(toClone);
    }

    private void pushMessage(HashSet<Subscription> subscriptions, String xml, String pushMessageName) {

        //TODO: Denne håndterer ikke de ulike subscriptionene, kun om denne meldingen er sendt til noen, og får dermed ikke med seg nye subscriptions (det er nok ikke noe stort problem for ET meldinger som kommer ofte, men kanskje SX...)
        Long ifPresent = alreadySentCache.get(xml);
        if (ifPresent != null) {
            long diffInSecs = (System.currentTimeMillis() - ifPresent) / 1000;
            logger.debug("skips message since it has already been \"pushed\" {} seconds ago", diffInSecs);
            return;
        }

        alreadySentCache.set(xml, System.currentTimeMillis());

        PushMessage pushMessage = new PushMessage();
        pushMessage.setMessagename(pushMessageName);
        pushMessage.setXmlPayload(xml);
        pushMessage.setNode(hostname);

        for (Subscription subscription : subscriptions) {
            logger.debug("PUSH ({}): to subscription name: {}, pushAddress: {}\n{}",
                    hostname, subscription.getName(), subscription.getPushAddress(), xml);
            if (StringUtils.startsWithIgnoreCase(subscription.getPushAddress(), "http://") ) {
                pushToHttp(subscription, pushMessage);
            } else {
                logger.debug("No push address for subscription with id='{}' - stores it in memory instead");
                storeMessageInMemory(subscription, pushMessage);
            }
        }
    }

    private void pushToHttp(Subscription subscription, PushMessage pushMessage) {
        RestTemplate restTemplate = new RestTemplate();
        URI uri = URI.create(subscription.getPushAddress());
        PushAcknowledge response = null;
        try {
            response = restTemplate.postForObject(uri, pushMessage, PushAcknowledge.class);
            logger.debug("Receive {} on push to {} for subscription with id {}",
                    response, subscription.getPushAddress(), subscription.getId());
        } catch (Exception e) {
            logger.warn("Could not push to {} for subscription with id {}",
                    subscription.getPushAddress(), subscription.getId(), e);
        }
        if (response == FORGET_ME) {
            logger.info("Receive {} on push to {} and removes subscription with id {}",
                    FORGET_ME, subscription.getPushAddress(), subscription.getId());
            remove(subscription.getId());
        } else if (response == OK) {
            subscription.resetFailedPushCounter();
            subscriptions.put(subscription.getId(), subscription); //to distribute change to other hazelcast nodes
        } else {
            logger.info("Unexpected response on push '{}' - increase failed push counter for subscription wih id {}",
                    response, subscription.getId());
            int failedPushCounter = subscription.increaseFailedPushCounter();
            if (failedPushCounter > 3) {
                logger.info("Removes subscription with id {} after {} failed push attempts",
                        subscription.getId(), failedPushCounter);
                remove(subscription.getId());
            } else {
                subscriptions.put(subscription.getId(), subscription); //to distribute change to other hazelcast nodes
            }
        }
    }

    private void storeMessageInMemory(Subscription subscription, PushMessage pushMessage) {
        List<PushMessage> pushMessages = pushMessagesMemoryStore.get(subscription.getId());
        if (pushMessages == null) {
            pushMessages = new ArrayList<>();
        }
        pushMessages.add(pushMessage);
        pushMessagesMemoryStore.put(subscription.getId(), pushMessages);
    }

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private String getPushMessageName(EstimatedCall estimatedCall, EstimatedVehicleJourney estimatedVehicleJourney) {
        List<NaturalLanguageStringStructure> stopPointNames = estimatedCall.getStopPointNames();
        String name = stopPointNames.isEmpty() ? estimatedCall.getStopPointRef().getValue() : stopPointNames.get(0).getValue();
        String vehicleJourney = estimatedVehicleJourney.getVehicleRef() == null ? "null" : estimatedVehicleJourney.getVehicleRef().getValue();
        LineRef lineRef = estimatedVehicleJourney.getLineRef();
        String line = "null";
        if (lineRef != null && StringUtils.containsIgnoreCase(lineRef.getValue(), ":Line:")) {
            line = StringUtils.substringAfterLast(lineRef.getValue(), ":");
        }
        return LocalDateTime.now().format(formatter) + " ET " + line + " " + vehicleJourney + " " + name;
    }

    private String getPushMessageName(PtSituationElement ptSituationElement) {
        String situationNumber = ptSituationElement.getSituationNumber() == null ? "null" : ptSituationElement.getSituationNumber().getValue();
        return LocalDateTime.now().format(formatter) + "_SX_" + situationNumber + ".xml";
    }

    private String toXMLString(Object element) {
        try {
            return siriMarshaller.prettyPrintNoNamespaces(element);
        } catch (JAXBException | XMLStreamException e) {
            logger.warn("Error marshalling object", e);
            return "ERROR: " + e.getMessage();
        }
    }

}
