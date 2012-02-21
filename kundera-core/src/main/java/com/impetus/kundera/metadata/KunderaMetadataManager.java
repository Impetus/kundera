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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.proxy.EntityEnhancerFactory;
import com.impetus.kundera.proxy.LazyInitializerFactory;

/**
 * The Class KunderaMetadataManager.
 *
 * @author amresh.singh
 */
public class KunderaMetadataManager
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(KunderaMetadataManager.class);

    /**
     * Gets the persistence unit metadata.
     *
     * @param persistenceUnit the persistence unit
     * @return the persistence unit metadata
     */
    public static PersistenceUnitMetadata getPersistenceUnitMetadata(String persistenceUnit)
    {
        return KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit);
    }

    /**
     * Gets the metamodel.
     *
     * @param persistenceUnit the persistence unit
     * @return the metamodel
     */
    public static MetamodelImpl getMetamodel(String persistenceUnit)
    {
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        MetamodelImpl metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(persistenceUnit);
        if (metamodel == null)
        {
            metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    Constants.COMMON_ENTITY_METADATAS);
        }

        return metamodel;
    }

    /**
     * Gets the metamodel.
     *
     * @param persistenceUnits the persistence units
     * @return the metamodel
     */
    public static MetamodelImpl getMetamodel(String entityName, String... persistenceUnits)
    {
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        MetamodelImpl metamodel = null;
        for (String pu : persistenceUnits)
        {
            metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(pu);

            if (metamodel != null && metamodel.getEntityClass(entityName) != null)
            {
                return metamodel;
            }
        }

        //If not in specified in any persistence unit, it should be here.
        metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                Constants.COMMON_ENTITY_METADATAS);

        return metamodel;
    }

    /**
     * Gets the entity metadata.
     *
     * @param persistenceUnit the persistence unit
     * @param entityClass the entity class
     * @return the entity metadata
     */
    public static EntityMetadata getEntityMetadata(String persistenceUnit, Class entityClass)
    {
        return getMetamodel(persistenceUnit).getEntityMetadata(entityClass);
    }

    /**
     * Finds ands returns Entity metadata for a given array of PUs.
     *
     * @param entityClass the entity class
     * @param persistenceUnits the persistence units
     * @return the entity metadata
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
                + ". If your entity is for RDBMS, make sure you put fully qualified entity class"
                + " name under <class></class> tag in persistence.xml for RDBMS "
                + "persistence unit. Returning null value.");
        return null;

    }

    /**
     * Gets the lazy initializer factory.
     *
     * @return the lazy initializer factory
     */
    public static LazyInitializerFactory getLazyInitializerFactory()
    {
        return KunderaMetadata.INSTANCE.getCoreMetadata().getLazyInitializerFactory();
    }

    /**
     * Gets the entity enhancer factory.
     *
     * @return the entity enhancer factory
     */
    public static EntityEnhancerFactory getEntityEnhancerFactory()
    {
        return KunderaMetadata.INSTANCE.getCoreMetadata().getEnhancedProxyFactory();
    }

}
