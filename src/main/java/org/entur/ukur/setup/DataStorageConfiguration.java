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

package org.entur.ukur.setup;


import org.entur.ukur.service.DataStorageHazelcastService;
import org.entur.ukur.service.DataStorageService;
import org.entur.ukur.service.ExtendedHazelcastService;
import org.entur.ukur.service.MetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DataStorageConfiguration {

    private final ExtendedHazelcastService extendedHazelcastService;
    private MetricsService metricsService;

    @Autowired
    public DataStorageConfiguration(ExtendedHazelcastService extendedHazelcastService, MetricsService metricsService) {
        this.extendedHazelcastService = extendedHazelcastService;
        this.metricsService = metricsService;
    }

    @Bean
    public DataStorageService createDataStorageService() {

        DataStorageService dataStorageService = new DataStorageHazelcastService(
                extendedHazelcastService.subscriptionIdsPerStopPoint(),
                extendedHazelcastService.subscriptions(),
                extendedHazelcastService.currentJourneys());


        metricsService.registerGauge(MetricsService.GAUGE_SUBSCRIPTIONS, dataStorageService::getNumberOfSubscriptions);
        metricsService.registerGauge(MetricsService.GAUGE_LIVE_JOURNEYS, dataStorageService::getNumberOfCurrentJourneys);

        return dataStorageService;
    }
}
