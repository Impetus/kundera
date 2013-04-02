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
package com.impetus.kundera.metadata.model;

import java.util.HashMap;
import java.util.Map;

/**
 * The Class KunderaMetadata.
 * 
 * @author amresh.singh
 */
public class KunderaMetadata
{
    /* Metadata for Kundera core */
    /** The core metadata. */
    private CoreMetadata coreMetadata;

    /* User application specific metadata */
    /** The application metadata. */
    private ApplicationMetadata applicationMetadata;

    /* Client specific persistence unit specific metadata */
    /** The client metadata. */
    private Map<String, ClientMetadata> clientMetadata = new HashMap<String, ClientMetadata>();

    /** The Constant INSTANCE. */
    public static final KunderaMetadata INSTANCE = new KunderaMetadata();

    /**
     * Instantiates a new kundera metadata.
     */
    private KunderaMetadata()
    {

    }

    /*
     * public static synchronized KunderaMetadata getInstance() { if (instance
     * == null) { instance = new KunderaMetadata(); } return instance; }
     */
    /**
     * Gets the application metadata.
     * 
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
     * Gets the core metadata.
     * 
     * @return the coreMetadata
     */
    public CoreMetadata getCoreMetadata()
    {
        return coreMetadata;
    }

    /**
     * Sets the application metadata.
     * 
     * @param applicationMetadata
     *            the applicationMetadata to set
     */
    public void setApplicationMetadata(ApplicationMetadata applicationMetadata)
    {
        this.applicationMetadata = applicationMetadata;
    }

    /**
     * Sets the core metadata.
     * 
     * @param coreMetadata
     *            the coreMetadata to set
     */
    public void setCoreMetadata(CoreMetadata coreMetadata)
    {
        this.coreMetadata = coreMetadata;
    }

    /**
     * Gets the client metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the client metadata
     */
    public ClientMetadata getClientMetadata(String persistenceUnit)
    {
        return clientMetadata.get(persistenceUnit);
    }

    /**
     * Adds the client metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param clientMetadata
     *            the client metadata
     */
    public void addClientMetadata(String persistenceUnit, ClientMetadata clientMetadata)
    {
        this.clientMetadata.put(persistenceUnit, clientMetadata);
    }

    public void unloadKunderaMetadata(final String pu)
    {
        ClientMetadata clientMetadata = getClientMetadata(pu);
        if (clientMetadata != null)
        {
            this.clientMetadata.remove(pu);
            clientMetadata.setClientImplementor(null);
            clientMetadata.setIndexImplementor(null);
            clientMetadata.setLuceneIndexDir(null);
            clientMetadata = null;
        }
        // this.clientMetadata.clear();
        this.getApplicationMetadata().unloadApplicationMatadata(pu);
    }
}
