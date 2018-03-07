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


import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.entur.ukur.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DataStorageConfiguration {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ExtendedHazelcastService extendedHazelcastService;
    private final MetricsService metricsService;
    private boolean useDatastore;

    @Autowired
    public DataStorageConfiguration(ExtendedHazelcastService extendedHazelcastService,
                                    MetricsService metricsService,
                                    @Value("${ukur.datastore.useDatastore:false}") boolean useDatastore) {
        this.extendedHazelcastService = extendedHazelcastService;
        this.metricsService = metricsService;
        this.useDatastore = useDatastore;
    }

    @Bean
    public DataStorageService createDataStorageService() {


        DataStorageService dataStorageService;
        if (useDatastore) {
            logger.info("Creates a DataStorageService on Google Datastore service using Application Default Credentials");
            //TODO: Consider using LocalDatastoreHelper to run a local emulator when not running in the cloud
            Datastore service = DatastoreOptions.getDefaultInstance().getService();
            dataStorageService = new GoogleDatastoreService(
                    service,
                    extendedHazelcastService.currentJourneys());
        } else {
            logger.info("Creates a DataStorageService only on Hazelcast");
            dataStorageService = new DataStorageHazelcastService(
                    extendedHazelcastService.subscriptionIdsPerStopPoint(),
                    extendedHazelcastService.subscriptions(),
                    extendedHazelcastService.currentJourneys());

        }

        metricsService.registerGauge(MetricsService.GAUGE_SUBSCRIPTIONS, dataStorageService::getNumberOfSubscriptions);
        metricsService.registerGauge(MetricsService.GAUGE_LIVE_JOURNEYS, dataStorageService::getNumberOfCurrentJourneys);

        return dataStorageService;
    }
}
