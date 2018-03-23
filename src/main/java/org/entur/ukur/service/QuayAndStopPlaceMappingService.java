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

package org.entur.ukur.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class QuayAndStopPlaceMappingService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final MetricsService metricsService;
    private HashMap<String, Collection<String>> stopPlaceIdToQuayIds = new HashMap<>();
    private HashMap<String, String> quayIdToStopPlaceId = new HashMap<>();

    @Autowired
    public QuayAndStopPlaceMappingService(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @PostConstruct
    public void registerMetrics() {
        metricsService.registerGauge(MetricsService.GAUGE_STOPPLACES, this::getNumberOfStopPlaces);
    }

    public void updateStopsAndQuaysMap(Map<String, Collection<String>> stopPlacesAndQuays) {
        HashMap<String, Collection<String>> newStopPlaceIdToQuayIds = new HashMap<>(stopPlacesAndQuays);
        HashMap<String, String> newQuayIdToStopPlaceId = new HashMap<>();
        for (Map.Entry<String, Collection<String>> stopAndQuays : newStopPlaceIdToQuayIds.entrySet()) {
            for (String quayId : stopAndQuays.getValue()) {
                newQuayIdToStopPlaceId.put(quayId, stopAndQuays.getKey());
            }
        }
        stopPlaceIdToQuayIds = newStopPlaceIdToQuayIds;
        quayIdToStopPlaceId = newQuayIdToStopPlaceId;
    }

    public String mapQuayToStopPlace(String quayId) {
        String stopPlaceid = quayIdToStopPlaceId.get(quayId);
        if (stopPlaceid == null) {
            logger.warn("Did not find quayId '{}' on any stopplace", quayId);
        }
        return stopPlaceid;
    }

    public Collection<String> mapStopPlaceToQuays(String stopPlaceId) {
        Collection<String> quayIds = stopPlaceIdToQuayIds.get(stopPlaceId);
        if (quayIds == null) {
            logger.warn("Did not find any stopPlace with stopPlaceId '{}'", stopPlaceId);
            return Collections.emptySet();
        }
        return quayIds;
    }

    public long getNumberOfStopPlaces() {
        return stopPlaceIdToQuayIds.size();
    }

    public HashMap<String, Collection<String>> getAllStopPlaces() {
        return new HashMap<>(stopPlaceIdToQuayIds);
    }
}
