/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.configure;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.thoughtworks.xstream.XStream;

/**
 * Abstract ptoperty reader parse xml or properties on the basis of
 * {@code PropertyType}
 * 
 * @author Kuldeep Mishra
 * 
 */
public abstract class AbstractPropertyReader
{
    /** The log instance. */
    private Log log = LogFactory.getLog(AbstractPropertyReader.class);

    /** The xStream instance */
    private XStream xStream;

    protected PersistenceUnitMetadata puMetadata;

    /**
     * Reads property file which is given in persistence unit
     * 
     * @param pu
     */
    public void read(String pu)
    {
        puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        String propertyFileName = puMetadata != null ? puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;

        // if (propertyFileName == null)
        // {
        // log.warn("No property file found in class path, kundera will use default property");
        // }
        // else
        // {
        if (propertyFileName != null && PropertyType.value(propertyFileName).equals(PropertyType.xml))
        {
            onXml(onParseXML(propertyFileName, puMetadata));
        }
        else
        {
            onProperties(onParseProperties(propertyFileName, puMetadata));
        }
        // }
    }

    /**
     * If property file is xml.
     * 
     * @param propertyFileName
     * @param puMetadata
     * @return
     */
    private ClientProperties onParseXML(String propertyFileName, PersistenceUnitMetadata puMetadata)
    {
        InputStream inStream = puMetadata.getClassLoader().getResourceAsStream(propertyFileName);
        xStream = getXStreamObject();
        if (inStream != null)
        {
            Object o = xStream.fromXML(inStream);
            return (ClientProperties) o;
        }
        return null;
    }

    /**
     * If property file is properties.
     * 
     * @param propertyFileName
     * @param puMetadata
     * @return
     */
    private Properties onParseProperties(String propertyFileName, PersistenceUnitMetadata puMetadata)
    {
        log.warn("Use of Properties file is Depricated ,please use xml format instaed ");
        Properties properties = new Properties();
        InputStream inStream = propertyFileName != null ? puMetadata.getClassLoader().getResourceAsStream(
                propertyFileName) : null;

        if (inStream != null)
        {
            try
            {
                properties.load(inStream);
                return properties;
            }
            catch (IOException e)
            {
                log.warn("error in loading properties , caused by :" + e.getMessage());
                throw new KunderaException(e);
            }
        }
        return null;
    }

    /**
     * get XStream Object.
     * 
     * @return XStream object.
     */
    private XStream getXStreamObject()
    {
        if (xStream == null)
        {
            XStream stream = new XStream();
            stream.alias("clientProperties", ClientProperties.class);
            stream.alias("dataStore", ClientProperties.DataStore.class);
            stream.alias("schema", ClientProperties.DataStore.Schema.class);
            stream.alias("table", ClientProperties.DataStore.Schema.Table.class);
            stream.alias("dataCenter", ClientProperties.DataStore.Schema.DataCenter.class);
            stream.alias("connection", ClientProperties.DataStore.Connection.class);
            stream.alias("server", ClientProperties.DataStore.Connection.Server.class);
            return stream;
        }
        else
        {
            return xStream;
        }
    }

    /**
     * property type emun.
     * 
     * @author Kuldeep Mishra
     * 
     */
    private enum PropertyType
    {
        xml, properties;

        private static final String DELIMETER = ".";

        /**
         * Check for allowed property format.
         * 
         * @param propertyFileName
         * @return
         */
        static PropertyType value(String propertyFileName)
        {
            if (isXml(propertyFileName))
            {
                return xml;
            }
            else if (isProperties(propertyFileName))
            {
                return properties;
            }
            throw new IllegalArgumentException("unsupported property provided format:" + propertyFileName);
        }

        /**
         * Check for property is xml.
         * 
         * @param propertyFileName
         * @return
         */
        private static boolean isXml(String propertyFileName)
        {
            return propertyFileName.endsWith(DELIMETER + xml);
        }

        /**
         * Check for property is properties.
         * 
         * @param propertyFileName
         * @return
         */
        private static boolean isProperties(String propertyFileName)
        {
            return propertyFileName.endsWith(DELIMETER + properties);
        }
    }

    /**
     * If property is xml.
     * 
     * @param cp
     */
    protected abstract void onXml(ClientProperties cp);

    /**
     * If property is properties.
     * 
     * @param props
     */
    protected abstract void onProperties(Properties props);
}
