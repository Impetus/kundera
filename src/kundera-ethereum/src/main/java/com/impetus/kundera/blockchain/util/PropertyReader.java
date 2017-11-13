/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.blockchain.util;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;

/**
 * The Class PropertyReader.
 * 
 * @author devender.yadav
 */
public class PropertyReader
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyReader.class);

    /** The kundera blockchain props. */
    private static Map<String, String> kunderaBlockchainProps;

    /**
     * Instantiates a new property reader.
     *
     * @param fileName
     *            the file name
     */
    public PropertyReader(String fileName)
    {
        kunderaBlockchainProps = getProperties(fileName);
    }

    /**
     * Gets the properties.
     *
     * @param fileName
     *            the file name
     * @return the properties
     */
    private static Map<String, String> getProperties(String fileName)
    {
        if (kunderaBlockchainProps == null || kunderaBlockchainProps.isEmpty())
        {
            Configuration config = null;
            try
            {
                config = new PropertiesConfiguration(fileName);

            }
            catch (ConfigurationException ce)
            {
                LOGGER.error("Not able to load properties from  " + fileName + " file. ", ce);
                throw new KunderaException("Not able to load properties from  " + fileName + " file. ", ce);
            }
            kunderaBlockchainProps = (Map) ConfigurationConverter.getProperties(config);
            LOGGER.info("Properties loaded from " + fileName + " file. Properties: " + kunderaBlockchainProps);
        }
        return kunderaBlockchainProps;
    }

    /**
     * Gets the property.
     *
     * @param key
     *            the key
     * @return the property
     */
    public String getProperty(String key)
    {
        return kunderaBlockchainProps.get(key);
    }

    /**
     * Gets the all properties.
     *
     * @return the all properties
     */
    public Map<String, String> getAllProperties()
    {
        return kunderaBlockchainProps;
    }

}
