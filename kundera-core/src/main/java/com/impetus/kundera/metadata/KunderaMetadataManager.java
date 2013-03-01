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
package com.impetus.kundera.metadata;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

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
     * @param persistenceUnit
     *            the persistence unit
     * @return the persistence unit metadata
     */
    public static PersistenceUnitMetadata getPersistenceUnitMetadata(String persistenceUnit)
    {
        if (persistenceUnit != null)
        {
            return KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(persistenceUnit);
        }
        return null;
    }

    /**
     * Gets the metamodel.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the metamodel
     */
    public static MetamodelImpl getMetamodel(String persistenceUnit)
    {
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        MetamodelImpl metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(persistenceUnit);

        return metamodel;
    }

    /**
     * Gets the metamodel.
     * 
     * @param persistenceUnits
     *            the persistence units
     * @return the metamodel
     */
    public static MetamodelImpl getMetamodel(String... persistenceUnits)
    {
        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;

        MetamodelImpl metamodel = null;
        for (String pu : persistenceUnits)
        {
            metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(pu);

            if (metamodel != null)
            {
                return metamodel;
            }
        }

        // FIXME: I need to verify this why we need common entity metadata now!
        // if (metamodel == null)
        // {
        // metamodel = (MetamodelImpl)
        // kunderaMetadata.getApplicationMetadata().getMetamodel(
        // Constants.COMMON_ENTITY_METADATAS);
        // }
        return metamodel;
    }

    /**
     * Gets the entity metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param entityClass
     *            the entity class
     * @return the entity metadata
     */
    public static EntityMetadata getEntityMetadata(String persistenceUnit, Class entityClass)
    {
        return getMetamodel(persistenceUnit).getEntityMetadata(entityClass);
    }

    /**
     * Finds ands returns Entity metadata for a given array of PUs.
     * 
     * @param entityClass
     *            the entity class
     * @param persistenceUnits
     *            the persistence units
     * @return the entity metadata
     */
    public static EntityMetadata getEntityMetadata(Class entityClass)
    {
        if (entityClass == null)
        {
            throw new KunderaException("Invalid class provided " + entityClass);
        }
        List<String> persistenceUnits = KunderaMetadata.INSTANCE.getApplicationMetadata().getMappedPersistenceUnit(
                entityClass);

        // persistence units will only have more than 1 persistence unit in case
        // of RDBMS.
        if (persistenceUnits != null)
        {
            for (String pu : persistenceUnits)
            {
                MetamodelImpl metamodel = getMetamodel(pu);
                EntityMetadata metadata = metamodel.getEntityMetadata(entityClass);
                if (metadata != null && metadata.getPersistenceUnit().equals(pu))
                {
                    return metadata;
                }
            }
        }
        if (log.isDebugEnabled())
            log.warn("No Entity metadata found for the class " + entityClass
                    + ". Any CRUD operation on this entity will fail."
                    + "If your entity is for RDBMS, make sure you put fully qualified entity class"
                    + " name under <class></class> tag in persistence.xml for RDBMS "
                    + "persistence unit. Returning null value.");

        throw new KunderaException("Unable to load entity metadata for :" + entityClass);
    }
}
