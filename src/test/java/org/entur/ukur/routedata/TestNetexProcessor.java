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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@SuppressWarnings("WeakerAccess")
public class TestNetexProcessor {

    private static JAXBContext jaxbContext;
//    private String timeZone;

//    public LocalDateTime publicationTimestamp;
    public Map<String, JourneyPattern> journeyPatternsById;
//    public Map<String, ServiceJourney> serviceJourneyByPatternId;
//    public Set<String> calendarServiceIds;
//    public Map<String, DayType> dayTypeById;
//    public Map<String, DayTypeAssignment> dayTypeAssignmentByDayTypeId;
//    public Map<String, OperatingPeriod> operatingPeriodById;
//    public Map<String, Boolean> dayTypeAvailable;
    public Map<String, String> quayIdByStopPointRef;
//    public Map<String, Route> routeById;
    public Map<String, ServiceJourney> serviceJourneyById;

    private ZipFile zipFile;


    static {
        try {
            jaxbContext = JAXBContext.newInstance(PublicationDeliveryStructure.class);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    public TestNetexProcessor(File file) throws IOException {
        zipFile = new ZipFile(file, ZipFile.OPEN_READ);
        journeyPatternsById = new HashMap<>();
//        serviceJourneyByPatternId = new HashMap<>();
//        calendarServiceIds = new HashSet<>();
//        dayTypeById = new HashMap<>();
//        dayTypeAssignmentByDayTypeId = new HashMap<>();
//        operatingPeriodById = new HashMap<>();
//        dayTypeAvailable = new HashMap<>();
        quayIdByStopPointRef = new HashMap<>();
//        routeById = new HashMap<>();
        serviceJourneyById = new HashMap<>();
    }



    private Unmarshaller createUnmarshaller() throws JAXBException {
        return jaxbContext.createUnmarshaller();
    }

    public void loadFiles() {
        zipFile.stream().forEach(entry -> loadFile(entry, zipFile));
    }

    private byte[] entryAsBytes(ZipFile zipFile, ZipEntry entry) {
        try {
            return IOUtils.toByteArray(zipFile.getInputStream(entry));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void loadFile(ZipEntry entry, ZipFile zipFile) {
        try {
            byte[] bytesArray = entryAsBytes(zipFile, entry);

            PublicationDeliveryStructure value = parseXmlDoc(bytesArray);
            List<JAXBElement<? extends Common_VersionFrameStructure>> compositeFrameOrCommonFrames = value
                    .getDataObjects().getCompositeFrameOrCommonFrame();

//            publicationTimestamp = value.getPublicationTimestamp();

            for (JAXBElement frame : compositeFrameOrCommonFrames) {

                if (frame.getValue() instanceof CompositeFrame) {
                    CompositeFrame cf = (CompositeFrame) frame.getValue();
//                    VersionFrameDefaultsStructure frameDefaults = cf.getFrameDefaults();
//                    String timeZone = "GMT";
//                    if (frameDefaults != null && frameDefaults.getDefaultLocale() != null
//                            && frameDefaults.getDefaultLocale().getTimeZone() != null) {
//                        timeZone = frameDefaults.getDefaultLocale().getTimeZone();
//                    }
//                    setTimeZone(timeZone);

                    List<JAXBElement<? extends Common_VersionFrameStructure>> commonFrames = cf.getFrames().getCommonFrame();
                    for (JAXBElement commonFrame : commonFrames) {
                        loadServiceFrames(commonFrame);
//                        loadServiceCalendarFrames(commonFrame);
                        loadTimeTableFrames(commonFrame);
                    }
                }
            }
        } catch (JAXBException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private PublicationDeliveryStructure parseXmlDoc(byte[] bytesArray) throws JAXBException {
        JAXBElement<PublicationDeliveryStructure> root;
        ByteArrayInputStream stream = new ByteArrayInputStream(bytesArray);

        //noinspection unchecked
        root = (JAXBElement<PublicationDeliveryStructure>) createUnmarshaller().unmarshal(stream);

        return root.getValue();
    }

    // ServiceJourneys
    private void loadTimeTableFrames(JAXBElement commonFrame) {
        if (commonFrame.getValue() instanceof TimetableFrame) {
            TimetableFrame timetableFrame = (TimetableFrame) commonFrame.getValue();

            JourneysInFrame_RelStructure vehicleJourneys = timetableFrame.getVehicleJourneys();
            List<Journey_VersionStructure> datedServiceJourneyOrDeadRunOrServiceJourney = vehicleJourneys.getDatedServiceJourneyOrDeadRunOrServiceJourney();
            for (Journey_VersionStructure jStructure : datedServiceJourneyOrDeadRunOrServiceJourney) {
                if (jStructure instanceof ServiceJourney) {
//                    loadServiceIds((ServiceJourney) jStructure);
                    ServiceJourney sj = (ServiceJourney) jStructure;
                    serviceJourneyById.put(sj.getId(), sj);
//                    String journeyPatternId = sj.getJourneyPatternRef().getValue().getRef();
//                    JourneyPattern journeyPattern = journeyPatternsById.get(journeyPatternId);
//                    if (journeyPattern != null) {
//                        if (journeyPattern.getPointsInSequence().
//                                getPointInJourneyPatternOrStopPointInJourneyPatternOrTimingPointInJourneyPattern()
//                                .size() == sj.getPassingTimes().getTimetabledPassingTime().size()) {
//
//                            serviceJourneyByPatternId.put(journeyPatternId, sj);
//                        }
//                    }
                }
            }
        }
    }

//    private void loadServiceIds(ServiceJourney serviceJourney) {
//        DayTypeRefs_RelStructure dayTypes = serviceJourney.getDayTypes();
//        String serviceId = mapToServiceId(dayTypes);
//        // Add all unique service ids to map. Used when mapping calendars later.
//        calendarServiceIds.add(serviceId);
//    }

//    private static String mapToServiceId(DayTypeRefs_RelStructure dayTypes) {
//        StringBuilder serviceId = new StringBuilder();
//        boolean first = true;
//        for (JAXBElement dt : dayTypes.getDayTypeRef()) {
//            if (!first) {
//                serviceId.append("+");
//            }
//            first = false;
//            if (dt.getValue() instanceof DayTypeRefStructure) {
//                DayTypeRefStructure dayType = (DayTypeRefStructure) dt.getValue();
//                serviceId.append(dayType.getRef());
//            }
//        }
//        return serviceId.toString();
//    }

    // ServiceCalendar
//    private void loadServiceCalendarFrames(JAXBElement commonFrame) {
//        if (commonFrame.getValue() instanceof ServiceCalendarFrame) {
//            ServiceCalendarFrame scf = (ServiceCalendarFrame) commonFrame.getValue();
//
//            if (scf.getServiceCalendar() != null) {
//                DayTypes_RelStructure dayTypes = scf.getServiceCalendar().getDayTypes();
//                for (JAXBElement dt : dayTypes.getDayTypeRefOrDayType_()) {
//                    if (dt.getValue() instanceof DayType) {
//                        DayType dayType = (DayType) dt.getValue();
//                        dayTypeById.put(dayType.getId(), dayType);
//                    }
//                }
//            }
//
//            if (scf.getDayTypes() != null) {
//                List<JAXBElement<? extends DataManagedObjectStructure>> dayTypes = scf.getDayTypes()
//                        .getDayType_();
//                for (JAXBElement dt : dayTypes) {
//                    if (dt.getValue() instanceof DayType) {
//                        DayType dayType = (DayType) dt.getValue();
//                        dayTypeById.put(dayType.getId(), dayType);
//                    }
//                }
//            }
//
//            if (scf.getOperatingPeriods() != null) {
//                for (OperatingPeriod_VersionStructure operatingPeriodStruct : scf
//                        .getOperatingPeriods().getOperatingPeriodOrUicOperatingPeriod()) {
//                    OperatingPeriod operatingPeriod = (OperatingPeriod) operatingPeriodStruct;
//                    operatingPeriodById.put(operatingPeriod.getId(), operatingPeriod);
//                }
//            }
//
//            List<DayTypeAssignment> dayTypeAssignments = scf.getDayTypeAssignments().getDayTypeAssignment();
//
//            for (DayTypeAssignment dayTypeAssignment : dayTypeAssignments) {
//                String ref = dayTypeAssignment.getDayTypeRef().getValue().getRef();
//                Boolean available = dayTypeAssignment.isIsAvailable() == null ?
//                        true :
//                        dayTypeAssignment.isIsAvailable();
//
//                dayTypeAvailable.put(dayTypeAssignment.getId(), available);
//
//
//                dayTypeAssignmentByDayTypeId.put(ref, dayTypeAssignment);
//            }
//        }
//    }

    private void loadServiceFrames(JAXBElement commonFrame) {
        if (commonFrame.getValue() instanceof ServiceFrame) {
            ServiceFrame sf = (ServiceFrame) commonFrame.getValue();

            //stop assignments
            StopAssignmentsInFrame_RelStructure stopAssignments = sf.getStopAssignments();
            if (stopAssignments != null) {
                List<JAXBElement<? extends StopAssignment_VersionStructure>> assignments = stopAssignments
                        .getStopAssignment();
                for (JAXBElement assignment : assignments) {
                    if (assignment.getValue() instanceof PassengerStopAssignment) {
                        PassengerStopAssignment passengerStopAssignment =  (PassengerStopAssignment) assignment.getValue();

                        String quayRef = passengerStopAssignment.getQuayRef().getRef();

                        quayIdByStopPointRef.put(passengerStopAssignment.getScheduledStopPointRef().getValue().getRef(), quayRef);
                    }
                }
            }
//
//            //routes
//            RoutesInFrame_RelStructure routes = sf.getRoutes();
//            if (routes != null) {
//                List<JAXBElement<? extends LinkSequence_VersionStructure>> route_ = routes
//                        .getRoute_();
//                for (JAXBElement element : route_) {
//                    if (element.getValue() instanceof Route) {
//                        Route route = (Route) element.getValue();
//                        routeById.put(route.getId(), route);
//                    }
//                }
//            }


            //journeyPatterns
            JourneyPatternsInFrame_RelStructure journeyPatterns = sf.getJourneyPatterns();
            if (journeyPatterns != null) {
                List<JAXBElement<?>> journeyPattern_orJourneyPatternView = journeyPatterns
                        .getJourneyPattern_OrJourneyPatternView();
                for (JAXBElement pattern : journeyPattern_orJourneyPatternView) {
                    if (pattern.getValue() instanceof JourneyPattern) {
                        JourneyPattern journeyPattern = (JourneyPattern) pattern.getValue();
                        journeyPatternsById.put(journeyPattern.getId(), journeyPattern);
                    }
                }

            }
        }
    }

//    public void setTimeZone(String timeZone) {
//        this.timeZone = timeZone;
//    }

//    public String getTimeZone() {
//        return timeZone;
//    }
}
