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
import java.util.HashMap;
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
import com.impetus.kundera.metadata.entities.Article;
import com.impetus.kundera.metadata.entities.EmbeddableEntity;
import com.impetus.kundera.metadata.entities.SingularEntityEmbeddable;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.event.AddressEntity;
import com.impetus.kundera.property.PropertyAccessorHelper;

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

        
//        EntityMetadata invalidMetadata = KunderaMetadataManager.getEntityMetadata(AddressEntity.class);
//        Assert.assertNull(invalidMetadata);

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
    
    @Test
    public void testIsEmbeddedAttributeIndexable()
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(SingularEntityEmbeddable.class);
        Assert.assertNotNull(m);
        
        try
        {
            Assert.assertTrue(MetadataUtils.isEmbeddedAtributeIndexable(m.getEntityClazz().getDeclaredField("embeddableEntity")));
            Assert.assertTrue(MetadataUtils.isEmbeddedAtributeIndexable(m.getEntityClazz().getDeclaredField("embeddableEntityTwo")));
        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }  
        
    }
    
    @Test
    public void testPopulateColumnAndSuperColumnMaps()
    {
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(SingularEntityEmbeddable.class);
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);
        
        Assert.assertNotNull(columnNameToFieldMap);
        Assert.assertNotNull(superColumnNameToFieldMap);
        Assert.assertFalse(columnNameToFieldMap.isEmpty());
        Assert.assertFalse(superColumnNameToFieldMap.isEmpty());
    }
    
    @Test
    public void testIsColumnInEmbeddableIndexable()
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(SingularEntityEmbeddable.class);
        Assert.assertNotNull(m);
        
        try
        {
            Assert.assertTrue(MetadataUtils.isColumnInEmbeddableIndexable(m.getEntityClazz().getDeclaredField("embeddableEntityTwo"), "field"));
            Assert.assertFalse(MetadataUtils.isColumnInEmbeddableIndexable(m.getEntityClazz().getDeclaredField("embeddableEntityTwo"), "name"));
            
            Assert.assertTrue(MetadataUtils.isColumnInEmbeddableIndexable(m.getEntityClazz().getDeclaredField("embeddableEntity"), "field"));            
        }
        catch (SecurityException e)
        {
           Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testContainsBasicElementCollectionField()
    {
        EntityMetadata m1 = KunderaMetadataManager.getEntityMetadata(Article.class);
        Assert.assertNotNull(m1);
        Assert.assertTrue(MetadataUtils.containsBasicElementCollectionField(m1));
        
        EntityMetadata m2 = KunderaMetadataManager.getEntityMetadata(SingularEntityEmbeddable.class);
        Assert.assertNotNull(m2);
        Assert.assertFalse(MetadataUtils.containsBasicElementCollectionField(m2));
    }
    
    @Test
    public void testIsBasicElementCollectionField()
    {       
        
        try
        {                       
            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(Article.class);
            
            Field f = Article.class.getDeclaredField("body");
            Assert.assertFalse(MetadataUtils.isBasicElementCollectionField(f));
            
            Field f1 = Article.class.getDeclaredField("comments");
            Assert.assertTrue(MetadataUtils.isBasicElementCollectionField(f1));
            
            Field f2 = Article.class.getDeclaredField("tags");
            Assert.assertTrue(MetadataUtils.isBasicElementCollectionField(f2));
            
            Field f3 = Article.class.getDeclaredField("likedBy");
            Assert.assertTrue(MetadataUtils.isBasicElementCollectionField(f3));
            
        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }
    }
}
