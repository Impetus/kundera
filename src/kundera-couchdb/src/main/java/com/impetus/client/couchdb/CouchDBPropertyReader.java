/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.couchdb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * CouchDB Property Reader.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /** log instance. */
    private static Logger log = LoggerFactory.getLogger(CouchDBPropertyReader.class);

    /** MongoDB schema metadata instance. */
    public static CouchDBSchemaMetadata csmd;

    /**
     * Instantiates a new couch db property reader.
     * 
     * @param externalProperties
     *            the external properties
     * @param puMetadata
     *            the pu metadata
     */
    public CouchDBPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        csmd = new CouchDBSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus
     * .kundera.configure.ClientProperties)
     */
    @Override
    protected void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            csmd.setClientProperties(cp);
        }
    }

    /**
     * The Class CouchDBSchemaMetadata.
     * 
     * @author Kuldeep Mishra
     */
    public class CouchDBSchemaMetadata
    {

        /** The client properties. */
        private ClientProperties clientProperties;

        /**
         * Gets the client properties.
         * 
         * @return the client properties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        /**
         * Sets the client properties.
         * 
         * @param clientProperties
         *            the new client properties
         */
        public void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }
    }
}