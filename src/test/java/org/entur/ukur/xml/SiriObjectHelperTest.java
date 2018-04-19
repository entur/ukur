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

package org.entur.ukur.xml;

import org.junit.Test;
import uk.org.siri.siri20.DatedVehicleJourneyRef;
import uk.org.siri.siri20.DestinationRef;
import uk.org.siri.siri20.SituationVersion;

import java.math.BigInteger;

import static org.entur.ukur.xml.SiriObjectHelper.getBigIntegerValue;
import static org.entur.ukur.xml.SiriObjectHelper.getStringValue;
import static org.entur.ukur.xml.SiriObjectHelper.getValue;
import static org.junit.Assert.*;

public class SiriObjectHelperTest {

    @Test
    public void getStringValueTests() {
        assertNull(getStringValue(null));
        assertNull(getStringValue("")); //will log error (as there is no getValue method on String...)
        assertNull(getStringValue(new DatedVehicleJourneyRef()));
        DestinationRef destinationRef = new DestinationRef();
        destinationRef.setValue("value");
        assertEquals("value", getStringValue(destinationRef));
    }

    @Test
    public void getValueTests() {
        assertNull(getValue(null, String.class));
        assertNull(getValue("", String.class)); //will log error (as there is no getValue method on String...)
        assertNull(getValue(new DatedVehicleJourneyRef(), String.class));
        DestinationRef destinationRef = new DestinationRef();
        destinationRef.setValue("value");
        assertEquals("value", getValue(destinationRef, String.class));
        SituationVersion situationVersion = new SituationVersion();
        situationVersion.setValue(BigInteger.valueOf(111));
        assertEquals(BigInteger.valueOf(111), getValue(situationVersion, BigInteger.class));
    }

    @Test
    public void getBigIntegerValueTests() {
        assertNull(getBigIntegerValue(null));
        assertNull(getBigIntegerValue("")); //will log error (as there is no getValue method on String...)
        SituationVersion situationVersion = new SituationVersion();
        situationVersion.setValue(BigInteger.valueOf(111));
        assertEquals(BigInteger.valueOf(111), getBigIntegerValue(situationVersion));
    }
}