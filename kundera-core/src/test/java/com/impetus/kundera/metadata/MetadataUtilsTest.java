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

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.Persistence;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.entities.EmbeddableEntity;
import com.impetus.kundera.metadata.entities.SingularEntityEmbeddable;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.event.AddressEntity;

/**
 * @author vivek.mishra
 * junit for {@link MetadataUtils}.
 *
 */
public class MetadataUtilsTest
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
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());

        EmbeddableType embeddableType = metaModel.embeddable(EmbeddableEntity.class);

        Map<String, Field> embeddableMap = MetadataUtils.createColumnsFieldMap(entityMetadata, embeddableType);
        Assert.assertNotNull(embeddableMap);
        Assert.assertEquals(1, embeddableMap.size());

        embeddableMap = MetadataUtils.createSuperColumnsFieldMap(entityMetadata);
        Assert.assertNotNull(embeddableMap);
        Assert.assertEquals(2, embeddableMap.size());

        Assert.assertTrue(MetadataUtils.defaultTransactionSupported(persistenceUnit));

        EntityType<SingularEntityEmbeddable> entityType = metaModel.entity(SingularEntityEmbeddable.class);

        Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance((Field) entityType.getAttribute(
                "embeddableEntity").getJavaMember());

        Assert.assertNotNull(embeddedObject);
        Assert.assertTrue(embeddedObject.getClass().isAssignableFrom(EmbeddableEntity.class));

        try
        {
            MetadataUtils.getEmbeddedCollectionInstance((Field) entityType.getAttribute("embeddableEntity")
                    .getJavaMember());
            Assert.fail("Should have gone to catch block!");
        }
        catch (InvalidEntityDefinitionException iedx)
        {
            Assert.assertNotNull(iedx);
        }

        
        EntityMetadata invalidMetadata = KunderaMetadataManager.getEntityMetadata(AddressEntity.class);
        Assert.assertNull(invalidMetadata);

    }

    @Test
    public void testSerializeKeys()
    {
        Set<String> foreignKeys = new HashSet<String>();
        foreignKeys.add("key1");
        foreignKeys.add("key2");
        foreignKeys.add("key3");

        String serializedKey = MetadataUtils.serializeKeys(foreignKeys);
        Assert.assertNotNull(serializedKey);
        Assert.assertEquals("key3~key2~key1", serializedKey);
    }

    @Test
    public void testDeserializeKeys()
    {
        final String serializedKey = "key1~key2~key3";

        Set<String> foreignKeys = MetadataUtils.deserializeKeys(serializedKey);
        Assert.assertNotNull(foreignKeys);
        Assert.assertEquals(3, foreignKeys.size());
    }

    @Test
    public void testWithNullEntity()
    {
        try
        {
            KunderaMetadataManager.getEntityMetadata(null);
            Assert.fail("Should have gone to catch block!");
        }catch(KunderaException kex)
        {
            Assert.assertNotNull(kex);
        }
        
    }
}
