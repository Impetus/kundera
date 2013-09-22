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

/**
 * @author vivek.mishra
 * junit for {@link KunderaMetadataManager}.
 *
 */
public class KunderaMetadataManagerTest
{

    private String persistenceUnit = "patest";

    @Before
    public void setup()
    {
        Persistence.createEntityManagerFactory(persistenceUnit);
    }

    @Test
    public void test()
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(SingularEntityEmbeddable.class);
        Assert.assertNotNull(entityMetadata);
        entityMetadata = KunderaMetadataManager.getEntityMetadata(persistenceUnit, SingularEntityEmbeddable.class);
        Assert.assertNotNull(entityMetadata);
        
        try
        {
            entityMetadata = KunderaMetadataManager.getEntityMetadata(null);
            Assert.fail("Should have gone to catch block!");
        }catch(KunderaException kex)
        {
            Assert.assertNotNull(kex.getMessage());
        }
        
        entityMetadata = KunderaMetadataManager.getEntityMetadata(GeneratedIdStrategyIdentity.class);
        Assert.assertNull(entityMetadata);
        
        Metamodel metaModel = KunderaMetadataManager.getMetamodel(persistenceUnit);
        Assert.assertNotNull(metaModel);

        metaModel = KunderaMetadataManager.getMetamodel(persistenceUnit,"KunderaTests");
        Assert.assertNotNull(metaModel);
        
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);
        
        Assert.assertNotNull(puMetadata);
        
        puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(null);
        
        Assert.assertNull(puMetadata);
        
        
    }

}
