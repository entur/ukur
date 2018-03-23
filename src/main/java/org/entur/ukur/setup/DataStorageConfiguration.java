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
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import org.apache.commons.lang3.StringUtils;
import org.entur.ukur.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;


@Configuration
public class DataStorageConfiguration {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ExtendedHazelcastService extendedHazelcastService;
    private final MetricsService metricsService;
    private boolean useDatastore;

    @Autowired
    public DataStorageConfiguration(ExtendedHazelcastService extendedHazelcastService,
                                    MetricsService metricsService,
                                    @Value("${ukur.datastore.useDatastore:true}") boolean useDatastore) {
        this.extendedHazelcastService = extendedHazelcastService;
        this.metricsService = metricsService;
        this.useDatastore = useDatastore;
    }

    @Bean
    public DataStorageService createDataStorageService() throws IOException, InterruptedException {
        DataStorageService dataStorageService;
        if (useDatastore) {
            //TODO: Consider using LocalDatastoreHelper to run a local emulator when not running in the cloud
            //Forslag: Dersom det finnes en GOOGLE_APPLICATION_CREDENTIALS env variabel gjør vi som under - ellers starter vi en emulator med LocalDatastoreHelper...

            Datastore service;
            String google_application_credentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (StringUtils.isNotBlank(google_application_credentials)) {
                service = DatastoreOptions.getDefaultInstance().getService();
                logger.info("Found GOOGLE_APPLICATION_CREDENTIALS as environment varable and instantiates a Datastore service based on those!");
            } else {
                LocalDatastoreHelper helper = LocalDatastoreHelper.create(1.0);
                helper.start();
                service = helper.getOptions().toBuilder().setNamespace("LocalTesting").build().getService();
                logger.info("Uses LocalDatastoreHelper to emulate a Datastore service");
                logger.warn("This node is not part of any datastore cluster and data is not persisted");
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        logger.info("Shuts down LocalDatastoreHelper...");
                        helper.stop();
                        logger.info("...LocalDatastoreHelper is stopped");
                    } catch (Exception e) {
                        logger.error("Could not shut down LocalDatastoreHelper", e);
                    }
                }));
            }
            logger.info("Creates a DataStorageService on Google Datastore");
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
