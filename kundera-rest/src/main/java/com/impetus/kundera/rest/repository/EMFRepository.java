/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.rest.repository;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;

/**
 * Repository for holding Application Tokens and {@link EntityManagerFactory} 
 * @author amresh.singh
 */
public class EMFRepository
{
    
    /** The Constant INSTANCE. */
    public static final EMFRepository INSTANCE = new EMFRepository();
    
    private Map<String, EntityManagerFactory> emfMap;

    /**
     * @return the emfMap
     */
    public Map<String, EntityManagerFactory> getEmfMap()
    {
        return emfMap;
    }
    
    public EntityManagerFactory getEMF(String applicationToken) {
        if(emfMap == null)  {
            return null;
        } else {
            return emfMap.get(applicationToken);
        }
    }

    /**
     * @param emfMap the emfMap to set
     */
    public void setEmfMap(Map<String, EntityManagerFactory> emfMap)
    {
        this.emfMap = emfMap;
    }
    
    /**
     * @param emfMap the emfMap to set
     */
    public void addEmf(String applicationToken, EntityManagerFactory emf)
    {
        if(emfMap == null) {
            emfMap = new HashMap<String, EntityManagerFactory>();
        }
        emfMap.put(applicationToken, emf);
    }
    
    public void removeEMF(String applicationToken) {
        if(emfMap != null) {
            emfMap.remove(applicationToken);
        }
    }
    
    

}
