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
import org.entur.ukur.xml.NoNamespaceIndentingXMLStreamWriter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.siri.siri20.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SubscriptionManager {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private HashMap<String, Set<Subscription>> subscriptionsPerStopPoint = new HashMap<>();
    private JAXBContext jaxbContext;
    private Cache<String, Long> alreadySentCache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .build();

    public SubscriptionManager() {
        try {
            jaxbContext = JAXBContext.newInstance(Siri.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Problem initializing JAXBContext", e);
        }
    }

    public void addSusbcription(Subscription subscription) {
        if (subscription == null || subscription.getToStopPoints().isEmpty() || subscription.getFromStopPoints().isEmpty()) {
            throw new IllegalArgumentException("Illegal subscription");
        }
        HashSet<String> subscribedStops = new HashSet<>();
        subscribedStops.addAll(subscription.getFromStopPoints());
        subscribedStops.addAll(subscription.getToStopPoints());
        for (String stoppoint : subscribedStops) {
            Set<Subscription> subscriptions = subscriptionsPerStopPoint.computeIfAbsent(stoppoint, k -> new HashSet<>());
            subscriptions.add(subscription);
        }
    }

    public Set<Subscription> getSubscriptionsForStopPoint(String stopPointRef) {
        return subscriptionsPerStopPoint.getOrDefault(stopPointRef, Collections.emptySet());
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

        Long ifPresent = alreadySentCache.getIfPresent(xml); //kanskje ikke godt nok...
        if (ifPresent != null) {
            long diffInSecs = (System.currentTimeMillis() - ifPresent) / 1000;
            logger.debug("skips message since it has already been \"pushed\" {} seconds ago", diffInSecs);
            return;
        }

        for (Subscription subscription : subscriptions) {
            alreadySentCache.put(xml, System.currentTimeMillis());
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
            logger.info("PUSH: to subscription name: {}\n{}", subscription.getName(), xml);
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
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            StringWriter stringWriter = new StringWriter();
            XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(stringWriter);
            jaxbMarshaller.marshal(element, new NoNamespaceIndentingXMLStreamWriter(writer));
            return stringWriter.getBuffer().toString();
        } catch (JAXBException | XMLStreamException e) {
            logger.warn("Error marshalling object", e);
            return "ERROR: " + e.getMessage();
        }
    }

}
