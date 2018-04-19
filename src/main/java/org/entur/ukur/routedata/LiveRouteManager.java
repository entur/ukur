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

import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.QuayAndStopPlaceMappingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.org.siri.siri20.EstimatedVehicleJourney;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;

@Service
public class LiveRouteManager {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private DataStorageService dataStorageService;
    private QuayAndStopPlaceMappingService quayAndStopPlaceMappingService;


    @Autowired
    public LiveRouteManager(DataStorageService dataStorageService, QuayAndStopPlaceMappingService quayAndStopPlaceMappingService) {
        this.dataStorageService = dataStorageService;
        this.quayAndStopPlaceMappingService = quayAndStopPlaceMappingService;
    }

    public void updateJourney(EstimatedVehicleJourney journey) {
        //TODO: Using VehicleRef like this will probably only work with NSB/BaneNOR
        if (journey != null && journey.getVehicleRef() != null) {
            if (Boolean.TRUE.equals(journey.isIsCompleteStopSequence())) {
                LiveJourney lj = new LiveJourney(journey, quayAndStopPlaceMappingService);
                if (lj.getLastArrivalTime() == null) {
                    logger.info("Got EstimatedVehicleJourney (VehicleRef={}) that we could not read LastArrivalTime from - skips it", getStringValue(journey.getVehicleRef()));
                } else {
                    dataStorageService.putCurrentJourney(lj);
                    logger.trace("Set journey with VehicleRef={}", lj.getVehicleRef());
                }
            } else {
                logger.warn("Got EstimatedVehicleJourney with incomplete StopSequence - skips it... (VehicleRef={})", getStringValue(journey.getVehicleRef()));
            }
        }
    }

    @SuppressWarnings({"unused", "WeakerAccess"}) //used from camel quartz route
    public void flushOldJourneys() {
        ZonedDateTime now = ZonedDateTime.now().minusMinutes(15);//Keeps journeys 15 minutes after their last arrival
        dataStorageService.removeJourneysOlderThan(now);
    }

    @SuppressWarnings({"unused", "WeakerAccess"}) //used from camel rest route
    public Collection<LiveJourney> getJourneys(String lineref) {
        if (lineref == null) {
            return null;
        }
        ArrayList<LiveJourney> routes  = new ArrayList<>();
        for (LiveJourney route : getJourneys()) {
            if (lineref.equals(route.getLineRef())) {
                routes.add(route);
            }
        }
        logger.debug("Found {} routes for lineref={}", routes.size(), lineref);
        return routes;
    }

    @SuppressWarnings({"unused", "WeakerAccess"}) //used from camel rest route
    public Collection<LiveJourney> getJourneys() {
        Collection<LiveJourney> routes = Collections.unmodifiableCollection(dataStorageService.getCurrentJourneys());
        logger.debug("There are totally {} routes ", routes.size());
        return routes;
    }

}
