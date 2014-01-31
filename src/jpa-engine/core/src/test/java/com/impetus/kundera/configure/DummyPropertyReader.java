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

import java.util.Map;

import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Class Dummy Property Reader for test
 * 
 * @author Kuldeep Mishra
 * 
 */
public class DummyPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    public static DummySchemaMetadata dsmd;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus
     * .kundera.configure.ClientProperties)
     */

    public DummyPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        dsmd = new DummySchemaMetadata();
    }

    @Override
    public void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            dsmd.setClientProperties(cp);
        }

    }

    /**
     * Dummy schema metadata for test.
     * 
     * @author Kuldeep Mishra
     * 
     */
    public class DummySchemaMetadata
    {
        /**
         * client properties.
         */
        private ClientProperties clientProperties;

        /**
         * @return the clientProperties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        /**
         * @param clientProperties
         *            the clientProperties to set
         */
        private void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }
    }
}
