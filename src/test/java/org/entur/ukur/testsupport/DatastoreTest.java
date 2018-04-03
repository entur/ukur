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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public abstract class DatastoreTest {
    private static final LocalDatastoreHelper HELPER = LocalDatastoreHelper.create(1.0);
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DatastoreTest.class);

    protected Datastore datastore;

    /**
     * Starts the local Datastore emulator.
     */
    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException {
        logger.info("Starts HELPER...");
        HELPER.start();
        logger.info("...HELPER started");
    }

    /**
     * Initializes Datastore and cleans out any residual values.
     */
    @Before
    public void setUp() throws Exception {
        datastore = HELPER.getOptions().toBuilder().setNamespace("test").build().getService();
        StructuredQuery<Key> query = Query.newKeyQueryBuilder().build();
        QueryResults<Key> result = datastore.run(query);
        Key[] keys = Iterators.toArray(result, Key.class);
        logger.info("Deletes {} entries from the local emulated datastore before test", keys.length);
        datastore.delete(keys);
    }

    /**
     * Stops the local Datastore emulator.
     */
    @AfterClass
    public static void afterClass() throws IOException, InterruptedException, TimeoutException {
        logger.info("Stops HELPER...");
        HELPER.stop(Duration.ofSeconds(10));
        logger.info("...HELPER stopped");
    }
}
