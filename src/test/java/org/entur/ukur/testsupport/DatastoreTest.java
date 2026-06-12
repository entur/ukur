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

package org.entur.ukur.testsupport;

import com.google.cloud.datastore.*;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.collect.Iterators;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public abstract class DatastoreTest {
    private static final LocalDatastoreHelper HELPER = LocalDatastoreHelper.create(1.0);
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DatastoreTest.class);

    /*
     * Start the emulator exactly once per JVM (the first time any DatastoreTest subclass is
     * loaded) and stop it via a JVM shutdown hook. Doing this per test class - as @BeforeAll /
     * @AfterAll would - restarts the shared emulator for every class in the fork, and each stop
     * races the emulator's connection teardown, intermittently failing with
     * "Socket Unexpected end of file from server". One start/stop per fork removes that race.
     */
    static {
        try {
            logger.info("Starts HELPER...");
            HELPER.start();
            logger.info("...HELPER started");
        } catch (IOException | InterruptedException e) {
            throw new ExceptionInInitializerError(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stops HELPER...");
            try {
                HELPER.stop(Duration.ofSeconds(10));
                logger.info("...HELPER stopped");
            } catch (IOException | InterruptedException | TimeoutException e) {
                // The emulator commonly drops the connection while handling the /shutdown request
                // (e.g. "Socket Unexpected end of file from server") - the process is being killed
                // anyway, so an error here is not worth failing on.
                logger.warn("Ignoring error while stopping the local Datastore emulator: {}", e.toString());
            }
        }, "datastore-emulator-shutdown"));
    }

    protected Datastore datastore;

    /**
     * Initializes Datastore and cleans out any residual values.
     */
    @BeforeEach
    public void setUp() throws Exception {
        datastore = HELPER.getOptions().toBuilder().setNamespace("test").build().getService();
        StructuredQuery<Key> query = Query.newKeyQueryBuilder().build();
        QueryResults<Key> result = datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        logger.info("Deletes {} entries from the local emulated datastore before test", keys.length);
        datastore.delete(keys);
    }
}
