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
package com.impetus.kundera.metadata;

import org.apache.log4j.Logger;

import com.impetus.kundera.Constants;
import com.impetus.kundera.loader.MetamodelLoader;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.proxy.EntityEnhancerFactory;
import com.impetus.kundera.proxy.LazyInitializerFactory;

/**
 * @author amresh.singh
 * 
 */
public class KunderaMetadataManager
{
    private static Logger log = Logger.getLogger(KunderaMetadataManager.class);

    public static PersistenceUnitMetadata getPersistenceUnitMetadata(String persistenceUnit)
    {
        return KunderaMetadata.getInstance().getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit);
    }

    public static MetamodelImpl getMetamodel(String persistenceUnit)
    {
        KunderaMetadata kunderaMetadata = KunderaMetadata.getInstance();

        MetamodelImpl metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(persistenceUnit);
        if (metamodel == null)
        {
            metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    Constants.COMMON_ENTITY_METADATAS);
        }

        return metamodel;
    }

    public static EntityMetadata getEntityMetadata(String persistenceUnit, Class entityClass)
    {
        return getMetamodel(persistenceUnit).getEntityMetadata(entityClass);
    }

    /**
     * Finds ands returns Entity metadata for a given array of PUs
     * 
     * @param entityClass
     * @param persistenceUnits
     * @return
     */
    public static EntityMetadata getEntityMetadata(Class entityClass, String... persistenceUnits)
    {
        for (String pu : persistenceUnits)
        {
            MetamodelImpl metamodel = getMetamodel(pu);
            EntityMetadata metadata = metamodel.getEntityMetadata(entityClass);
            if (metadata != null)
            {
                return metadata;
            }
        }
        log.warn("Something is terribly wrong, No Entity metadata found for the class " + entityClass
                + ". Returning null value.");
        return null;

    }

    public static LazyInitializerFactory getLazyInitializerFactory()
    {
        return KunderaMetadata.getInstance().getCoreMetadata().getLazyInitializerFactory();
    }

    public static EntityEnhancerFactory getEntityEnhancerFactory()
    {
        return KunderaMetadata.getInstance().getCoreMetadata().getEnhancedProxyFactory();
    }

}
