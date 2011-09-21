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
package com.impetus.kundera.startup.model;


/**
 * 
 * @author amresh.singh
 */
public class KunderaMetadata
{
    private static KunderaMetadata instance;
    
    private KunderaMetadata() {
        
    }
    
    public static synchronized KunderaMetadata getInstance()
    {
        if (instance == null)
        {
            instance = new KunderaMetadata();
        }
        return instance;
    }
    
    /* Metadata for Kundera core */
    private CoreMetadata coreMetadata;
    
    /* Client specific metadata */
    private ClientMetadata clientMetadata;
    
    /* User application specific metadata */
    private ApplicationMetadata applicationMetadata;

    /**
     * @return the coreMetadata
     */
    public CoreMetadata getCoreMetadata()
    {
        return coreMetadata;
    }

    /**
     * @param coreMetadata the coreMetadata to set
     */
    public void setCoreMetadata(CoreMetadata coreMetadata)
    {
        this.coreMetadata = coreMetadata;
    }

    /**
     * @return the clientMetadata
     */
    public ClientMetadata getClientMetadata()
    {
        return clientMetadata;
    }

    /**
     * @param clientMetadata the clientMetadata to set
     */
    public void setClientMetadata(ClientMetadata clientMetadata)
    {
        this.clientMetadata = clientMetadata;
    }

    /**
     * @return the applicationMetadata
     */
    public ApplicationMetadata getApplicationMetadata()
    {
        if(applicationMetadata == null) {
            applicationMetadata = new ApplicationMetadata();
        }
        return applicationMetadata;
    }

    /**
     * @param applicationMetadata the applicationMetadata to set
     */
    public void setApplicationMetadata(ApplicationMetadata applicationMetadata)
    {
        this.applicationMetadata = applicationMetadata;
    }  

}
