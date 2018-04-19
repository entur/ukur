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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.math.BigInteger;

public class SiriObjectHelper {

    private static Logger logger = LoggerFactory.getLogger(SiriObjectHelper.class);

    public static String getStringValue(Object siriObjectWithValue) {
        return getValue(siriObjectWithValue, String.class);
    }

    public static BigInteger getBigIntegerValue(Object siriObjectWithValue) {
        return getValue(siriObjectWithValue, BigInteger.class);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getValue(Object siriObjectWithValue, Class<T> resultClass) {
        if (siriObjectWithValue == null) {
            return null;
        }
        try {
            Method getValueMethod = siriObjectWithValue.getClass().getMethod("getValue");
            return (T) getValueMethod.invoke(siriObjectWithValue);
        } catch (Exception e) {
            logger.error("Could not use reflection to invoke the getValue-method of an instance of class {}", siriObjectWithValue.getClass().getName(), e);
        }
        return null;
    }


}
