/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.ejb;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;
import javax.persistence.PostPersist;
import javax.persistence.PrePersist;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.loader.Configuration;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * Task for persisting entity into NoSQL Datastore
 * @author amresh.singh
 */
public class PersistTask implements Runnable
{
    /** The Constant log. */
    private static final Log log = LogFactory.getLog(PersistTask.class);
    
    EnhancedEntity e;
    EntityManagerImpl em;
    
    public PersistTask(EnhancedEntity e, EntityManagerImpl em) {
        this.e = e;
        this.em = em;
    }
    
    @Override
    public void run()
    {
        try
        {
            EntityMetadata metadata = this.em.getMetadataManager().getEntityMetadata(e.getEntity().getClass());
            
            //Check if persistenceUnit name is same as the parent entity, if not, it's a case of cross-store persistence
            String persistenceUnit = metadata.getPersistenceUnit();
            if(persistenceUnit != null && ! persistenceUnit.equals(em.getPersistenceUnitName())) {
               //TODO: Required to set client in EM, check?
                em.setClient(((EntityManagerImpl)new Configuration().getEntityManager(persistenceUnit)).getClient());            
            }
            
            metadata.setDBType(em.getClient().getType());
            // TODO: throw EntityExistsException if already exists

            // fire pre-persist events
            em.getEventDispatcher().fireEventListeners(metadata, e, PrePersist.class);

            // TODO uncomment
            em.getDataManager().persist(e, metadata);
            em.getIndexManager().write(metadata, e.getEntity());

            // fire post-persist events
            em.getEventDispatcher().fireEventListeners(metadata, e, PostPersist.class);
        }
        catch (PersistenceException e)
        {            
            log.error("Error while persisting entity into datastore, Details: " + e.getMessage());
            throw e;
        }
        catch (Exception e)
        {            
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
        
    }   
    
}
