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
package com.impetus.kundera.metadata.processor;

import java.lang.reflect.Field;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.type.AbstractManagedType;

/**
 * MetaModelBuilderTest.
 * 
 * @author vivek.mishra
 * 
 */
public class MetaModelBuilderTest
{

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Test construct.
     *
     * @param <X> the generic type
     * @param <T> the generic type
     */
    @Test
    public <X extends Class, T extends Object> void testConstruct()
    {
        X clazz = (X) SingularEntity.class;
        MetaModelBuilder builder = new MetaModelBuilder<X, T>(clazz);
        Field[] field = SingularEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(SingularEntity.class, f);
        }

        try
        {
            MetaModelBuilder.class.getDeclaredFields();
            Field managedTypeField = builder.getClass().getDeclaredField("managedType");
            if (!managedTypeField.isAccessible())
            {
                managedTypeField.setAccessible(true);
            }

            AbstractManagedType<X> managedType = (AbstractManagedType<X>) managedTypeField.get(builder);
            Assert.assertNotNull(managedType);
            Assert.assertEquals(3, managedType.getSingularAttributes().size());
            Assert.assertEquals("key", managedType.getSingularAttribute("key").getName());
            Assert.assertEquals("name", managedType.getSingularAttribute("name").getName());
            Assert.assertEquals("field", managedType.getSingularAttribute("field").getName());

        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

}
