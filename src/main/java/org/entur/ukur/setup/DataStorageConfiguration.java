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
import com.hazelcast.core.ITopic;
import org.entur.ukur.service.DataStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.IOException;


@Configuration
public class DataStorageConfiguration {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ITopic<String> subscriptionCacheRenewerTopic;

    @Autowired
    public DataStorageConfiguration(@Qualifier("subscriptionCacheRenewerTopic") ITopic<String> subscriptionCacheRenewerTopic) {
        this.subscriptionCacheRenewerTopic = subscriptionCacheRenewerTopic;
    }

    @Bean
    @Profile("gcp-datastore")
    public DataStorageService createDataStorageService()  {
        Datastore service = DatastoreOptions.getDefaultInstance().getService();
            logger.info("Found GOOGLE_APPLICATION_CREDENTIALS as environment varable and instantiates a Datastore service based on those!");

        logger.info("Creates a DataStorageService on Google Datastore");
        DataStorageService dataStorageService = new DataStorageService(
                service,
                subscriptionCacheRenewerTopic);
        logger.info("DataStorageService created");
        return dataStorageService;
    }

    @Bean
    @Profile("local-datastore")
    public DataStorageService createLocalDateStorage() throws IOException, InterruptedException {
        LocalDatastoreHelper helper = LocalDatastoreHelper.create(1.0);
        helper.start();
        Datastore service = helper.getOptions().toBuilder().setNamespace("LocalTesting").build().getService();
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

        return  new DataStorageService(
                service,
                subscriptionCacheRenewerTopic);
    }
}
