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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.thoughtworks.xstream.XStream;

/**
 * Abstract property reader parse xml or properties on the basis of
 * {@code PropertyType}
 * 
 * @author Kuldeep Mishra
 * 
 */
public abstract class AbstractPropertyReader
{
    /** The log instance. */
    private static final Logger log = LoggerFactory.getLogger(AbstractPropertyReader.class);

    /** The xStream instance */
    private XStream xStream;

    protected PersistenceUnitMetadata puMetadata;

    protected Map externalProperties;

    public AbstractPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        this.externalProperties = externalProperties;
        this.puMetadata = puMetadata;
    }

    /**
     * Reads property file which is given in persistence unit
     * 
     * @param pu
     */
    public void read(String pu)
    {

        String propertyFileName = null;
        if (puMetadata.getProperties() != null
                && puMetadata.getProperties().containsKey(PersistenceProperties.KUNDERA_CLIENT_PROPERTY))
        {
            propertyFileName = (String) puMetadata.getProperties().get(PersistenceProperties.KUNDERA_CLIENT_PROPERTY);
        }
        if (externalProperties != null && externalProperties.containsKey(PersistenceProperties.KUNDERA_CLIENT_PROPERTY))
        {
            propertyFileName = (String) externalProperties.get(PersistenceProperties.KUNDERA_CLIENT_PROPERTY);
        }
        
        if (propertyFileName == null)
        {
            propertyFileName = puMetadata != null ? puMetadata
                    .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;
        }
        if (propertyFileName != null && PropertyType.value(propertyFileName) != null
                && PropertyType.value(propertyFileName).equals(PropertyType.xml))
        {
            onXml(onParseXML(propertyFileName, puMetadata));
        }
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
        if (inStream == null)
        {
            propertyFileName = KunderaCoreUtils.resolvePath(propertyFileName);
            try
            {
                inStream = new FileInputStream(new File(propertyFileName));
            }
            catch (FileNotFoundException e)
            {
                log.warn("File {} not found, Caused by ", propertyFileName);
                return null;
            }

        }

        if (inStream != null)
        {
            xStream = getXStreamObject();
            Object o = xStream.fromXML(inStream);
            return (ClientProperties) o;
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
    protected enum PropertyType
    {
        xml, properties;

        private static final String DELIMETER = ".";

        /**
         * Check for allowed property format.
         * 
         * @param propertyFileName
         * @return
         */
        public static PropertyType value(String propertyFileName)
        {
            PropertyType type = null;
            if (isValid(propertyFileName, PropertyType.xml))
            {
                type = xml;
            }
            else if (isValid(propertyFileName, PropertyType.properties))
            {
                if (log.isWarnEnabled())
                {
                    log.warn("Support for .properties have been deprecated and no longer supported by Kundera");
                }
                type = properties;
            }
            else
            {
                log.warn("Invalid file format {} provided, returning null", propertyFileName);
            }

            return type;
        }

        /**
         * Check for property is xml.
         * 
         * @param propertyFileName
         * @return
         */
        private static boolean isValid(String propertyFileName, PropertyType type)
        {
            return propertyFileName.endsWith(DELIMETER + type);
        }
    }

    /**
     * If property is xml.
     * 
     * @param cp
     */
    protected abstract void onXml(ClientProperties cp);

    protected class AbstractSchemaMetadata
    {
        private ClientProperties clientProperties;

        /**
         * @param parseXML
         */
        public void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }

        /**
         * @return the clientProperties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        protected DataStore getDataStore(final String dataStoreName)
        {
            if (getClientProperties() != null)
            {
                if (getClientProperties().getDatastores() != null)
                {
                    for (DataStore dataStore : getClientProperties().getDatastores())
                    {
                        if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase(dataStoreName))
                        {
                            return dataStore;
                        }
                    }
                }

                if (log.isWarnEnabled())
                {
                    log.warn("No data store configuration found, returning null.");
                }
            }
            return null;
        }
    }
}