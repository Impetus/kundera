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
package com.impetus.kundera.metadata.model;

import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep Mishra
 * 
 */
public class EntityMetadataTest
{
    private String persistenceUnit = "metaDataTest";

    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = getEntityManagerFactory(null);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

    @Test
    public void test()
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), Employe.class);
        Assert.assertNotNull(entityMetadata);
        Assert.assertNotNull(entityMetadata.getIndexProperties());
        Assert.assertFalse(entityMetadata.getIndexProperties().isEmpty());
        Assert.assertEquals(2, entityMetadata.getIndexProperties().size());
        Assert.assertNotNull(entityMetadata.getIndexProperties().get("AGE"));
        Assert.assertNotNull(entityMetadata.getIndexProperties().get("EMP_NAME"));
        Assert.assertNull(entityMetadata.getIndexProperties().get("departmentData"));
        Assert.assertNotNull(entityMetadata.toString());

        Map<String, PropertyIndex> indexes = IndexProcessor.getIndexesOnEmbeddable(Department.class);
        Assert.assertNotNull(indexes);
        Assert.assertFalse(indexes.isEmpty());
        Assert.assertEquals(2, indexes.size());

        Assert.assertNotNull(indexes.get("email"));
        Assert.assertEquals("ASC", indexes.get("email").getIndexType());
        Assert.assertEquals(new Integer(Integer.MAX_VALUE), indexes.get("email").getMax());
        Assert.assertEquals(new Integer(Integer.MIN_VALUE), indexes.get("email").getMin());

        Assert.assertNotNull(indexes.get("location"));
        Assert.assertEquals("GEO2D", indexes.get("location").getIndexType());
        Assert.assertEquals(new Integer(200), indexes.get("location").getMax());
        Assert.assertEquals(new Integer(-200), indexes.get("location").getMin());

    }

    @Test
    public void testEmbeddedCollection()
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), KunderaUser.class);
        Assert.assertNotNull(entityMetadata);
        Assert.assertTrue(entityMetadata.getIndexProperties().isEmpty());
        Assert.assertEquals(EntityMetadata.Type.SUPER_COLUMN_FAMILY, entityMetadata.getType());
        Assert.assertNotNull(entityMetadata.toString());

        entityMetadata.setCounterColumnType(false);
        Assert.assertFalse(entityMetadata.isCounterColumnType());
    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory(persistenceUnit);
    }

    @Test
    public void testColumn()
    {
        try
        {
            Column column = new Column("EMP_NAME", Employe.class.getDeclaredField("empName"));
            column.setIndexable(true);
            Assert.assertTrue(column.isIndexable());
            Assert.assertEquals("empName", column.getField().getName());
            Assert.assertEquals("EMP_NAME", column.getName());

            column = new Column("AGE", Employe.class.getDeclaredField("age"), true);
            Assert.assertTrue(column.isIndexable());
            Assert.assertEquals("age", column.getField().getName());
            Assert.assertEquals("AGE", column.getName());
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
