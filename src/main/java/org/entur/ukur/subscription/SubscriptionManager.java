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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hazelcast.core.IMap;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.xml.SiriMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class SubscriptionManager {

    private IMap<String, Set<Subscription>> subscriptionsPerStopPoint;
    private IMap<String, List<PushMessage>> pushMessagesMemoryStore;
    private final SiriMarshaller siriMarshaller;

    //TODO: AlreadySent bør enten sjekkes i anshar-polle-ruta eller her om vi skal støtte nye subscriptions (men da må cachen "til hazelcast")
    private Cache<String, Long> alreadySentCache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .build();
    private String hostname;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    public SubscriptionManager(IMap<String, Set<Subscription>> subscriptionsPerStopPoint,
                               IMap<String, List<PushMessage>> pushMessagesMemoryStore,
                               SiriMarshaller siriMarshaller) {
        this.subscriptionsPerStopPoint = subscriptionsPerStopPoint;
        this.pushMessagesMemoryStore = pushMessagesMemoryStore;
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
    public Set<Subscription> listAll() {
        HashSet<Subscription> all = new HashSet<>();
        //TODO: Mer fornuftig implementasjon her etterhvert...
        Collection<Set<Subscription>> values = subscriptionsPerStopPoint.values();
        for (Set<Subscription> value : values) {
            all.addAll(value);
        }
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

    public void addSusbcription(Subscription subscription) {
        if (subscription == null || subscription.getToStopPoints().isEmpty() || subscription.getFromStopPoints().isEmpty()) {
            throw new IllegalArgumentException("Illegal subscription");
        }
        HashSet<String> subscribedStops = new HashSet<>();
        subscribedStops.addAll(subscription.getFromStopPoints());
        subscribedStops.addAll(subscription.getToStopPoints());
        for (String stoppoint : subscribedStops) {
            Set<Subscription> subscriptions = subscriptionsPerStopPoint.get(stoppoint);
            if (subscriptions == null) {
                subscriptions = new HashSet<>();
            }
            subscriptions.add(subscription);
            subscriptionsPerStopPoint.put(stoppoint, subscriptions);
        }
        Set<String> stopRefs = subscriptionsPerStopPoint.keySet();
        logger.debug("There are new subscriptions regarding {} unique stoppoints", stopRefs.size());
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef) {
        Set<Subscription> subscriptions = subscriptionsPerStopPoint.get(stopPointRef);
        if (subscriptions == null) {
            return Collections.emptySet();
        }
        return subscriptions;
    }

    public void notify(HashSet<Subscription> subscriptions, EstimatedCall estimatedCall, EstimatedVehicleJourney estimatedVehicleJourney) {
        String pushMessageFilename = getPushMessageFilename(estimatedCall, estimatedVehicleJourney);
        EstimatedVehicleJourney clone = clone(estimatedVehicleJourney);
        EstimatedVehicleJourney.EstimatedCalls ec = new EstimatedVehicleJourney.EstimatedCalls();
        ec.getEstimatedCalls().add(estimatedCall);
        clone.setEstimatedCalls(ec);            //TODO: Fjerner alle andre EstimatedCalls enn gjeldende for å begrense størrelse (mulig det er dumt...?)
        clone.setRecordedCalls(null);           //TODO: Fjerner RecordedCalls for å begrense størrelse (mulig det er dumt...?)
        clone.setIsCompleteStopSequence(null);  //TODO: Fjerner evt IsCompleteStopSequence så det ikke blir feil ihht spec
        pushMessage(subscriptions, toXMLString(clone), pushMessageFilename);
    }

    public void notify(HashSet<Subscription> subscriptions, PtSituationElement ptSituationElement) {
        ptSituationElement = clone(ptSituationElement);
        ptSituationElement.setAffects(null);//TODO: Fjerner affects for å begrense størrelse (mulig det er dumt...?)
        String xml = toXMLString(ptSituationElement);
        String pushMessageFilename = getPushMessageFilename(ptSituationElement);
        pushMessage(subscriptions, xml, pushMessageFilename);
    }

    private <T extends Serializable> T clone(T toClone) {
        return SerializationUtils.clone(toClone);
    }

    private void pushMessage(HashSet<Subscription> subscriptions, String xml, String pushMessageFilename) {

        Long ifPresent = alreadySentCache.getIfPresent(xml); //kanskje ikke godt nok - støtter dårlig nye subscriptions...
        if (ifPresent != null) {
            long diffInSecs = (System.currentTimeMillis() - ifPresent) / 1000;
            logger.debug("skips message since it has already been \"pushed\" {} seconds ago", diffInSecs);
            return;
        }

        alreadySentCache.put(xml, System.currentTimeMillis());
        for (Subscription subscription : subscriptions) {
//            writeMessageToFile(xml, pushMessageFilename, subscription);
            storeMessageInMemory(xml, pushMessageFilename, subscription);
            logger.info("PUSH: to subscription name: {}\n{}", subscription.getName(), xml);
        }
    }

    private void storeMessageInMemory(String xml, String pushMessageFilename, Subscription subscription) {
        List<PushMessage> pushMessages = pushMessagesMemoryStore.get(subscription.getId());
        if (pushMessages == null) {
            pushMessages = new ArrayList<>();
        }
        PushMessage pushMessage = new PushMessage();
        pushMessage.setMessagename(pushMessageFilename);
        pushMessage.setXmlPayload(xml);
        pushMessage.setNode(hostname);
        pushMessages.add(pushMessage);
        pushMessagesMemoryStore.put(subscription.getId(), pushMessages);
    }

    private void writeMessageToFile(String xml, String pushMessageFilename, Subscription subscription) {
        try {
            File folder = new File("target/pushmessages/" + subscription.getName());
            //noinspection ResultOfMethodCallIgnored
            folder.mkdirs();
            FileWriter fw = new FileWriter(new File(folder, pushMessageFilename));
            fw.write(xml);
            fw.close();
        } catch (IOException e) {
            logger.error("Could not write pushmessage file", e);
        }
    }

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");

    private String getPushMessageFilename(EstimatedCall estimatedCall, EstimatedVehicleJourney estimatedVehicleJourney) {
        List<NaturalLanguageStringStructure> stopPointNames = estimatedCall.getStopPointNames();
        String name = stopPointNames.isEmpty() ? estimatedCall.getStopPointRef().getValue().replaceAll(":", "-") : stopPointNames.get(0).getValue();
        String vehicleJourney = estimatedVehicleJourney.getVehicleRef() == null ? "null" : estimatedVehicleJourney.getVehicleRef().getValue();
        LineRef lineRef = estimatedVehicleJourney.getLineRef();
        String line = "null";
        if (lineRef != null && StringUtils.containsIgnoreCase(lineRef.getValue(), ":Line:")) {
            line = StringUtils.substringAfterLast(lineRef.getValue(), ":");
        }
        String filename = LocalDateTime.now().format(formatter) + "_ET_" + line + "_" + vehicleJourney + "_" + name + ".xml";
        return filename.replaceAll(" ", "");
    }

    private String getPushMessageFilename(PtSituationElement ptSituationElement) {
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
