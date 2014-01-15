/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.impetus.kundera.utils;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;

/**
 * @author vivek.mishra junit for {@link KunderaCoreUtils}
 * 
 */
public class KunderaCoreUtilsTest
{

    @Test
    public void getExternalProperties()
    {
        Map<String, Object> propertiesMap = new HashMap<String, Object>();

        // Assert with empty map
        String[] persistenceUnits = new String[] { "metaDataTest", "patest" };

        Assert.assertNull(KunderaCoreUtils.getExternalProperties("patest", propertiesMap, persistenceUnits));
        Assert.assertTrue(KunderaCoreUtils.getExternalProperties("patest", propertiesMap, null).isEmpty());

        Map<String, Object> puProperties = new HashMap<String, Object>();
        puProperties.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");

        propertiesMap.put("patest", puProperties);

        Map<String, Object> persistenceUnitProps = KunderaCoreUtils.getExternalProperties("patest", propertiesMap,
                persistenceUnits);
        Assert.assertNotNull(persistenceUnitProps);
        Assert.assertEquals(puProperties, persistenceUnitProps);

        propertiesMap.put("patest", null);
        Assert.assertNull(KunderaCoreUtils.getExternalProperties("patest", propertiesMap, persistenceUnits));
    }

}
