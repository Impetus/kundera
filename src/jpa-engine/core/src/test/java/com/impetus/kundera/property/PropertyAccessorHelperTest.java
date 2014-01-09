/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.property;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.entity.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_1_M;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.persistence.context.CacheBase;
import com.impetus.kundera.property.accessor.StringAccessor;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * @author amresh.singh
 *
 */
public class PropertyAccessorHelperTest
{   

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#set(java.lang.Object, java.lang.reflect.Field, byte[])}.
     */
    @Test
    public void testSetObjectFieldByteArray()
    {
        PersonnelDTO person = new PersonnelDTO();    
        try
        {
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("personId"), "1".getBytes());
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("firstName"), "Amresh".getBytes());
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("lastName"), "Singh".getBytes());
            
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("Amresh", person.getFirstName());
            Assert.assertEquals("Singh", person.getLastName());
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#set(java.lang.Object, java.lang.reflect.Field, java.lang.String)}.
     */
    @Test
    public void testSetObjectFieldString()
    {
        PersonnelDTO person = new PersonnelDTO();    
        try
        {
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("personId"), "1");
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("firstName"), "Amresh");
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("lastName"), "Singh");
            
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("Amresh", person.getFirstName());
            Assert.assertEquals("Singh", person.getLastName());
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#set(java.lang.Object, java.lang.reflect.Field, java.lang.Object)}.
     */
    @Test
    public void testSetObjectFieldObject()
    {
        PersonnelDTO person = new PersonnelDTO();    
        try
        {
            Object id = "1";
            Object fn = "Amresh";
            Object ln = "Singh";
            
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("personId"), id);
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("firstName"), fn);
            PropertyAccessorHelper.set(person, person.getClass().getDeclaredField("lastName"), ln);
            
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("Amresh", person.getFirstName());
            Assert.assertEquals("Singh", person.getLastName());
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getObject(java.lang.Object, java.lang.reflect.Field)}.
     */
    @Test
    public void testGetObjectObjectField()
    {
        PersonnelDTO person = new PersonnelDTO("1", "Amresh", "Singh");
        
        try
        {
            Object id = PropertyAccessorHelper.getObject(person, person.getClass().getDeclaredField("personId"));
            Object fn = PropertyAccessorHelper.getObject(person, person.getClass().getDeclaredField("firstName"));
            Object ln = PropertyAccessorHelper.getObject(person, person.getClass().getDeclaredField("lastName"));
            
            Assert.assertEquals("1", id);
            Assert.assertEquals("Amresh", fn);
            Assert.assertEquals("Singh", ln);
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getObjectCopy(java.lang.Object, java.lang.reflect.Field)}.
     */
    @Test
    public void testGetObjectCopy()
    {
        PersonnelDTO person = new PersonnelDTO("1", "Amresh", "Singh");
        try
        {
            Object fn = PropertyAccessorHelper.getObjectCopy(person, person.getClass().getDeclaredField("firstName"));
            Assert.assertEquals("Amresh", fn);
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getString(java.lang.Object, java.lang.reflect.Field)}.
     */
    @Test
    public void testGetStringObjectField()
    {
        PersonnelDTO person = new PersonnelDTO("1", "Amresh", "Singh");        
        try
        {
            String id = PropertyAccessorHelper.getString(person, person.getClass().getDeclaredField("personId"));
            String fn = PropertyAccessorHelper.getString(person, person.getClass().getDeclaredField("firstName"));
            String ln = PropertyAccessorHelper.getString(person, person.getClass().getDeclaredField("lastName"));
            
            Assert.assertEquals("1", id);
            Assert.assertEquals("Amresh", fn);
            Assert.assertEquals("Singh", ln);
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#get(java.lang.Object, java.lang.reflect.Field)}.
     */
    @Test
    public void testGet()
    {
        PersonnelDTO person = new PersonnelDTO("1", "Amresh", "Singh");        
        try
        {
            byte[] id = PropertyAccessorHelper.get(person, person.getClass().getDeclaredField("personId"));
            byte[] fn = PropertyAccessorHelper.get(person, person.getClass().getDeclaredField("firstName"));
            byte[] ln = PropertyAccessorHelper.get(person, person.getClass().getDeclaredField("lastName"));
            
            Assert.assertEquals("1", new StringAccessor().fromBytes(String.class, id));
            Assert.assertEquals("Amresh", new StringAccessor().fromBytes(String.class, fn));
            Assert.assertEquals("Singh", new StringAccessor().fromBytes(String.class, ln));
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getId(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)}.
     */
    @Test
    public void testGetId()
    {
 
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#setId(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object)}.
     */
    @Test
    public void testSetIdObjectEntityMetadataObject()
    {
 
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#setId(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata, byte[])}.
     */
    @Test
    public void testSetIdObjectEntityMetadataByteArray()
    {
 
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getObject(java.lang.Object, java.lang.String)}.
     */
    @Test
    public void testGetObjectObjectString()
    {
        PersonnelDTO person = new PersonnelDTO("1", "Amresh", "Singh");        
        try
        {
            Object id = PropertyAccessorHelper.getObject(person, "personId");
            Object fn = PropertyAccessorHelper.getObject(person, "firstName");
            Object ln = PropertyAccessorHelper.getObject(person, "lastName");
            
            Assert.assertEquals("1", id);
            Assert.assertEquals("Amresh", fn);
            Assert.assertEquals("Singh", ln);
        }
        catch (SecurityException e)
        {            
            Assert.fail(e.getMessage());
        }        
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getGenericClass(java.lang.reflect.Field)}.
     */
    @Test
    public void testGetGenericClass()
    {
        Assert.assertNull(PropertyAccessorHelper.getGenericClass(null));
        try
        {
            Class<?> genericClass = PropertyAccessorHelper.getGenericClass(PhotographerUni_1_M_1_M.class.getDeclaredField("albums"));
            Assert.assertEquals(AlbumUni_1_M_1_M.class, genericClass);            
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getGenericClasses(java.lang.reflect.Field)}.
     */
    @Test
    public void testGetGenericClasses()
    {
        Assert.assertNull(PropertyAccessorHelper.getGenericClass(null));
        try
        {
            List<Class<?>> genericClasses = PropertyAccessorHelper.getGenericClasses(CacheBase.class.getDeclaredField("nodeMappings"));
            
            Assert.assertNotNull(genericClasses);
            Assert.assertFalse(genericClasses.isEmpty());
            Assert.assertEquals(2, genericClasses.size());
            Assert.assertEquals(String.class, genericClasses.get(0));
            Assert.assertEquals(Node.class, genericClasses.get(1));
                        
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getDeclaredFields(java.lang.reflect.Field)}.
     */
    @Test
    public void testGetDeclaredFields()
    {
        try
        {
            Field[] fields = PropertyAccessorHelper.getDeclaredFields(PhotographerUni_1_M_1_M.class.getDeclaredField("albums"));
            Assert.assertNotNull(fields);
            Assert.assertEquals(8, KunderaCoreUtils.countNonSyntheticFields(PhotographerUni_1_M_1_M.class));            
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#isCollection(java.lang.Class)}.
     */
    @Test
    public void testIsCollection()
    {
        try
        {
            Assert.assertTrue(PropertyAccessorHelper.isCollection(PhotographerUni_1_M_1_M.class.getDeclaredField("albums").getType()));
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getObject(java.lang.Class)}.
     */
    @Test
    public void testGetObjectClassOfQ()
    {
        Object o = PropertyAccessorHelper.getObject(PhotographerUni_1_M_1_M.class);
        Assert.assertNotNull(o);
        Assert.assertTrue(o instanceof PhotographerUni_1_M_1_M);
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#toBytes(java.lang.Object, java.lang.reflect.Field)}.
     */
    @Test
    public void testToBytesObjectField()
    {
        PersonnelDTO person = new PersonnelDTO("1", "Amresh", "Singh");        
        try
        {
            byte[] b = PropertyAccessorHelper.toBytes("1" , person.getClass().getDeclaredField("personId"));           
            Assert.assertEquals("1", new StringAccessor().fromBytes(String.class, b));            
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

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#toBytes(java.lang.Object, java.lang.Class)}.
     */
    @Test
    public void testToBytesObjectClass()
    {                
        try
        {
            byte[] b = PropertyAccessorHelper.toBytes("1" , String.class);           
            Assert.assertEquals("1", new StringAccessor().fromBytes(String.class, b));            
        }
        catch (SecurityException e)
        {            
            Assert.fail(e.getMessage());
        }       
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#fromSourceToTargetClass(java.lang.Class, java.lang.Class, java.lang.Object)}.
     */
    @Test
    public void testFromSourceToTargetClass()
    {
        try
        {
            Object o = PropertyAccessorHelper.fromSourceToTargetClass(String.class, String.class, "1");
            Assert.assertEquals("1", o);            
        }
        catch (SecurityException e)
        {            
            Assert.fail(e.getMessage());
        }    
    }

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#fromDate(java.lang.Class, java.lang.Class, java.lang.Object)}.
     */
    @Test
    public void testFromDate()
    {
        Object o = PropertyAccessorHelper.fromDate(String.class, Date.class, new Date(System.currentTimeMillis()));
        Assert.assertNotNull(o);
    }   

    /**
     * Test method for {@link com.impetus.kundera.property.PropertyAccessorHelper#getCollectionInstance(java.lang.reflect.Field)}.
     */
    @Test
    public void testGetCollectionInstance()
    {
        try
        {
            Collection c = PropertyAccessorHelper.getCollectionInstance(PhotographerUni_1_M_1_M.class.getDeclaredField("albums"));
            Assert.assertNotNull(c);
            Assert.assertTrue(c instanceof ArrayList);
            
            c = PropertyAccessorHelper.getCollectionInstance(CacheBase.class.getDeclaredField("headNodes"));
            Assert.assertNotNull(c);
            Assert.assertTrue(c instanceof HashSet);
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
