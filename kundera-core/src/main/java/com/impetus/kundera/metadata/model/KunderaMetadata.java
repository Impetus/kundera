/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.metadata.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author amresh.singh
 */
public class KunderaMetadata
{

    /* Metadata for Kundera core */
    private CoreMetadata coreMetadata;

    /* User application specific metadata */
    private ApplicationMetadata applicationMetadata;

    /* Client specific persistence unit specific metadata */
    private Map<String, ClientMetadata> clientMetadata = new HashMap<String, ClientMetadata>();

    public static final KunderaMetadata INSTANCE = new KunderaMetadata();

    private KunderaMetadata()
    {

    }

    /*
     * public static synchronized KunderaMetadata getInstance() { if (instance
     * == null) { instance = new KunderaMetadata(); } return instance; }
     */
    /**
     * @return the applicationMetadata
     */
    public ApplicationMetadata getApplicationMetadata()
    {
        if (applicationMetadata == null)
        {
            applicationMetadata = new ApplicationMetadata();
        }
        return applicationMetadata;
    }

    /**
     * @return the coreMetadata
     */
    public CoreMetadata getCoreMetadata()
    {
        return coreMetadata;
    }

    /**
     * @param applicationMetadata
     *            the applicationMetadata to set
     */
    public void setApplicationMetadata(ApplicationMetadata applicationMetadata)
    {
        this.applicationMetadata = applicationMetadata;
    }

    /**
     * @param coreMetadata
     *            the coreMetadata to set
     */
    public void setCoreMetadata(CoreMetadata coreMetadata)
    {
        this.coreMetadata = coreMetadata;
    }

    public ClientMetadata getClientMetadata(String persistenceUnit)
    {
        return clientMetadata.get(persistenceUnit);
    }

    public void addClientMetadata(String persistenceUnit, ClientMetadata clientMetadata)
    {
        this.clientMetadata.put(persistenceUnit, clientMetadata);
    }

}
