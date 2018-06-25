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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.junit.Ignore;
import org.junit.Test;
import org.onebusaway.gtfs.impl.GtfsRelationalDaoImpl;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.serialization.GtfsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertTrue;

@Ignore
public class GTFSReaderTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());


    @Test
    public void readNSB_GTFS() throws Exception {
        readGTFS("/home/jon/Documents/Entur/rb_nsb-aggregated-gtfs.zip");
    }

    @Test
    public void readNorwayGTFS() throws Exception {
        //disables trace logging as there is lots of service journeys:
        LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();
        logCtx.getLogger("org.entur").setLevel(Level.INFO);
        readGTFS("/home/jon/Documents/Entur/rb_norway-aggregated-gtfs.zip");
    }

    private void readGTFS(String path) throws Exception {
        long start = System.currentTimeMillis();
        File gtfsFile = new File(path);
        assertTrue(gtfsFile.exists());
        logger.info("Reading GTFS-data from {} - filesize: {} bytes", path, String.format("%,d", gtfsFile.length()));

        GtfsReader reader = new GtfsReader();
        GtfsRelationalDaoImpl dao;
        reader.setInputLocation(gtfsFile);
        dao = new GtfsRelationalDaoImpl();
        reader.setEntityStore(dao);
        reader.run();
        logger.info("GtfsReader completed after {} ms", String.format("%,d", (System.currentTimeMillis()-start)));

        Map<String, List<StopTime>> tripStops = new HashMap<>();
        Map<String, Set<String>> trainNumberTrips = new HashMap<>();
        Map<String, Set<String>> routeTrips = new HashMap<>();
//        Map<String, List<ServiceDate>> tripDates = new HashMap<>();
//        Map<String, String> parentStops = new HashMap<>();

//        for (Stop stop : dao.getAllStops()) {
//            if (stop.getParentStation() != null) {
//                parentStops.put(stop.getId().getId(), stop.getParentStation());
//            }
//        }

        for (StopTime stopTime : dao.getAllStopTimes()) {
            Trip trip = stopTime.getTrip();
            String serviceJourneyId = trip.getId().getId();
            if ("NSB:Authority:NSB".equals(trip.getId().getAgencyId())) {
                String trainNr = serviceJourneyId.substring(serviceJourneyId.lastIndexOf("-") + 1);
                Set<String> trips = trainNumberTrips.getOrDefault(trainNr, new HashSet<>());
                trips.add(serviceJourneyId);
                trainNumberTrips.put(trainNr, trips);
            }
            List<StopTime> stops = tripStops.getOrDefault(serviceJourneyId, new ArrayList<>());
            stops.add(stopTime);
            tripStops.put(serviceJourneyId, stops);

            String routeId = trip.getRoute().getId().getId();
            Set<String> trips = routeTrips.getOrDefault(routeId, new HashSet<>());
            trips.add(serviceJourneyId);
            routeTrips.put(routeId, trips);

//            List<ServiceCalendarDate> calendarDatesForServiceId = dao.getCalendarDatesForServiceId(trip.getServiceId());
//            tripDates.put(serviceJourneyId,
//                    calendarDatesForServiceId.stream()
//                            .map(ServiceCalendarDate::getDate)
//                            .collect(Collectors.toList())
//            );
        }

        //sort the stops according to their stop sequence number (if needed)
        for (Map.Entry<String, List<StopTime>> tripStopsEntry : tripStops.entrySet()) {
            List<StopTime> stops = tripStopsEntry.getValue();
            stops.sort(Comparator.comparingInt(StopTime::getStopSequence));
        }

        for (Map.Entry<String, Set<String>> entry: trainNumberTrips.entrySet()) {
            //tognummer -> serviceJourneyId
            String trainNr = entry.getKey();
            Set<String> serviceJourneyIds = entry.getValue();
            HashSet<String> uniqueStopsForTrainNr = new HashSet<>();
            for (String serviceJourneyId : serviceJourneyIds) {
                List<StopTime> stopTimes = tripStops.get(serviceJourneyId);
                StringBuilder sb = new StringBuilder();
                for (Iterator<StopTime> iterator = stopTimes.iterator(); iterator.hasNext(); ) {
                    Stop stop = iterator.next().getStop();
                    String quayId = stop.getId().getId();
                    String stopPlaceId = stop.getParentStation();
                    String stopName = stop.getName();
                    sb.append(quayId);
                    sb.append("(").append(stopName).append(" - ").append(stopPlaceId).append(")");
                    if (iterator.hasNext()) {
                        sb.append(", ");
                    }
                }
                uniqueStopsForTrainNr.add(sb.toString());
            }
//            logger.debug("Trainnumber {} has {} unique stopsequences for {} servicejourneys", trainNr, uniqueStopsForTrainNr.size(), serviceJourneyIds.size());
//            for (String stops : uniqueStopsForTrainNr) {
//                logger.debug(" -> "+stops);
//            }
        }

        for (Map.Entry<String, Set<String>> entry : routeTrips.entrySet()) {
            //lineref -> entry
            String lineRef = entry.getKey();
            Set<String> serviceJourneyIds = entry.getValue();
            HashSet<String> uniqueStopsForLineRef = new HashSet<>();
            for (String serviceJourneyId : serviceJourneyIds) {
                List<StopTime> stopTimes = tripStops.get(serviceJourneyId);
                StringBuilder sb = new StringBuilder();
                for (Iterator<StopTime> iterator = stopTimes.iterator(); iterator.hasNext(); ) {
                    StopTime stopTime = iterator.next();
                    Stop stop = stopTime.getStop();
                    String quayId = stop.getId().getId();
                    String stopPlaceId = stop.getParentStation();
                    String stopName = stop.getName();
//                    String direction = stopTime.getTrip().getDirectionId();
                    String direction = stopTime.getTrip().getTripHeadsign();
                    sb.append(quayId);
//                    sb.append("(").append(stopName).append(" - ").append(stopPlaceId).append(")");
                    sb.append("(").append(direction).append(")");
                    if (iterator.hasNext()) {
                        sb.append(", ");
                    }
                }
                uniqueStopsForLineRef.add(sb.toString());
            }
            logger.debug("LineRef {} has {} unique stopsequences for {} servicejourneys", lineRef, uniqueStopsForLineRef.size(), serviceJourneyIds.size());
            for (String stops : uniqueStopsForLineRef) {
                logger.debug(" -> "+stops);
            }
        }

        logger.info("Total processing took {} ms", String.format("%,d", (System.currentTimeMillis()-start)));
        logger.info("tripStops.size(): {}", tripStops.size());
        logger.info("trainNumberTrips.size(): {}", trainNumberTrips.size());
        logger.info("routeTrips.size(): {}", routeTrips.size());

        logger.info("trainNumberTrips keys: {}", trainNumberTrips.keySet());
        logger.info("routeTrips keys: {}", routeTrips.keySet());
        logger.info("tripStops keys: {}", tripStops.keySet());

        logger.info("trainNumberTrips: {}", trainNumberTrips);
        logger.info("routeTrips: {}", routeTrips);
//        logger.info("tripStops: {}", tripStops);
//        logger.info("tripDates.size(): {}", tripDates.size());
//        logger.info("parentStops.size(): {}", parentStops.size());
    }


}