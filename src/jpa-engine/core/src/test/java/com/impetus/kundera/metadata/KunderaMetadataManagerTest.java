/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.metadata;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Metamodel;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.entities.SingularEntityEmbeddable;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyIdentity;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author vivek.mishra junit for {@link KunderaMetadataManager}.
 * 
 */
public class KunderaMetadataManagerTest
{

    private String persistenceUnit = "patest";

    private EntityManagerFactory emf;

    private KunderaMetadata kunderaMetadata;

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
    }

    @Test
    public void test()
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                SingularEntityEmbeddable.class);
        Assert.assertNotNull(entityMetadata);
        entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, persistenceUnit,
                SingularEntityEmbeddable.class);
        Assert.assertNotNull(entityMetadata);

        try
        {
            entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (KunderaException kex)
        {
            Assert.assertNotNull(kex.getMessage());
        }

        entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, GeneratedIdStrategyIdentity.class);
        Assert.assertNull(entityMetadata);

        Metamodel metaModel = KunderaMetadataManager.getMetamodel(kunderaMetadata, persistenceUnit);
        Assert.assertNotNull(metaModel);

        metaModel = KunderaMetadataManager.getMetamodel(kunderaMetadata, persistenceUnit, "KunderaTests");
        Assert.assertNotNull(metaModel);

        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata,
                persistenceUnit);

        Assert.assertNotNull(puMetadata);

        puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata, null);

        Assert.assertNull(puMetadata);

    }

}
