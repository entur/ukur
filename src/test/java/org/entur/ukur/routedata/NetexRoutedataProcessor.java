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

package org.entur.ukur.routedata;

import org.apache.commons.io.IOUtils;
import org.rutebanken.netex.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class NetexRoutedataProcessor {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, String> journeyPatternIdByServiceJourneyId = new HashMap<>();
    private Map<String, List<PointInLinkSequence_VersionedChildStructure>> pointsInSequenceByJourneyPatternId = new HashMap<>();
    private Map<String, String> quayIdByStopPointRef = new HashMap<>();

    private static JAXBContext jaxbContext;

    static {
        try {
            jaxbContext = JAXBContext.newInstance(PublicationDeliveryStructure.class);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    public void loadNeTExZipFile(File file) throws IOException, JAXBException {
        logger.debug("Loads file {}", file.getPath());
        ZipFile zipFile = new ZipFile(file, ZipFile.OPEN_READ);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        zipFile.stream().forEach(entry -> loadFile(entry, zipFile, unmarshaller));
        zipFile.close();
    }

    public HashMap<String, Map<Integer, String>> getOrderedListOfQuaysForDatedVehicleJourneyRefs() {
        HashMap<String, Map<Integer, String>> orderedListOfQuaysForDatedVehicleJourneyRefs = new HashMap<>();
        for (Map.Entry<String, String> serviceJourneyIdAndJourneyPatternId : journeyPatternIdByServiceJourneyId.entrySet()) {
            List<PointInLinkSequence_VersionedChildStructure> pointsInSequence = pointsInSequenceByJourneyPatternId.get(serviceJourneyIdAndJourneyPatternId.getValue());
            HashMap<Integer, String> orderedListOfQuays = new HashMap<>(pointsInSequence.size());
            for (PointInLinkSequence_VersionedChildStructure point : pointsInSequence) {
                int order = point.getOrder().intValue();
                String stopPointRef = ((StopPointInJourneyPattern)point).getScheduledStopPointRef().getValue().getRef();
                String quay = quayIdByStopPointRef.get(stopPointRef);
                orderedListOfQuays.put(order, quay);
            }
            orderedListOfQuaysForDatedVehicleJourneyRefs.put(serviceJourneyIdAndJourneyPatternId.getKey(), orderedListOfQuays);
        }
        return orderedListOfQuaysForDatedVehicleJourneyRefs ;
    }

    private void loadFile(ZipEntry entry, ZipFile zipFile, Unmarshaller unmarshaller) {
        try {
            logger.trace("Loads entry {} with size {}", entry.getName(), entry.getSize());
            InputStream inputStream = zipFile.getInputStream(entry);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            inputStream.close();
            JAXBElement<PublicationDeliveryStructure> root;
            ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
            //noinspection unchecked
            root = (JAXBElement<PublicationDeliveryStructure>) unmarshaller.unmarshal(stream);
            PublicationDeliveryStructure value = root.getValue();
            List<JAXBElement<? extends Common_VersionFrameStructure>> compositeFrameOrCommonFrames = value.getDataObjects().getCompositeFrameOrCommonFrame();
            for (JAXBElement frame : compositeFrameOrCommonFrames) {
                if (frame.getValue() instanceof CompositeFrame) {
                    CompositeFrame cf = (CompositeFrame) frame.getValue();
                    List<JAXBElement<? extends Common_VersionFrameStructure>> commonFrames = cf.getFrames().getCommonFrame();
                    for (JAXBElement commonFrame : commonFrames) {
                        loadServiceFrames(commonFrame);
                        loadTimeTableFrames(commonFrame);
                    }
                }
            }
        } catch (JAXBException|IOException e) {
            //TODO: Improve errorhandling
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void loadTimeTableFrames(JAXBElement commonFrame) {
        if (commonFrame.getValue() instanceof TimetableFrame) {
            TimetableFrame timetableFrame = (TimetableFrame) commonFrame.getValue();

            JourneysInFrame_RelStructure vehicleJourneys = timetableFrame.getVehicleJourneys();
            List<Journey_VersionStructure> datedServiceJourneyOrDeadRunOrServiceJourney = vehicleJourneys.getDatedServiceJourneyOrDeadRunOrServiceJourney();
            for (Journey_VersionStructure jStructure : datedServiceJourneyOrDeadRunOrServiceJourney) {
                if (jStructure instanceof ServiceJourney) {
                    ServiceJourney sj = (ServiceJourney) jStructure;
                    journeyPatternIdByServiceJourneyId.put(sj.getId(), sj.getJourneyPatternRef().getValue().getRef());
                }
            }
        }
    }

    private void loadServiceFrames(JAXBElement commonFrame) {
        if (commonFrame.getValue() instanceof ServiceFrame) {
            ServiceFrame sf = (ServiceFrame) commonFrame.getValue();

            //stop assignments
            StopAssignmentsInFrame_RelStructure stopAssignments = sf.getStopAssignments();
            if (stopAssignments != null) {
                List<JAXBElement<? extends StopAssignment_VersionStructure>> assignments = stopAssignments.getStopAssignment();
                for (JAXBElement assignment : assignments) {
                    if (assignment.getValue() instanceof PassengerStopAssignment) {
                        PassengerStopAssignment passengerStopAssignment =  (PassengerStopAssignment) assignment.getValue();
                        String quayRef = passengerStopAssignment.getQuayRef().getRef();
                        quayIdByStopPointRef.put(passengerStopAssignment.getScheduledStopPointRef().getValue().getRef(), quayRef);
                    }
                }
            }

            //journeyPatterns
            JourneyPatternsInFrame_RelStructure journeyPatterns = sf.getJourneyPatterns();
            if (journeyPatterns != null) {
                List<JAXBElement<?>> journeyPattern_orJourneyPatternView = journeyPatterns.getJourneyPattern_OrJourneyPatternView();
                for (JAXBElement pattern : journeyPattern_orJourneyPatternView) {
                    if (pattern.getValue() instanceof JourneyPattern) {
                        JourneyPattern journeyPattern = (JourneyPattern) pattern.getValue();
                        List<PointInLinkSequence_VersionedChildStructure> pointsInSequence = journeyPattern.getPointsInSequence().getPointInJourneyPatternOrStopPointInJourneyPatternOrTimingPointInJourneyPattern();
                        pointsInSequenceByJourneyPatternId.put(journeyPattern.getId(), pointsInSequence);
                    }
                }

            }
        }
    }

}
