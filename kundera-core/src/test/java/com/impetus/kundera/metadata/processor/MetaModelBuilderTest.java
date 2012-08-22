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
import java.util.Map;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.Bindable.BindableType;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type.PersistenceType;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.type.AbstractIdentifiableType;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;

/**
 * MetaModelBuilderTest.
 * 
 * @author vivek.mishra
 * 
 */
public class MetaModelBuilderTest
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(MetaModelBuilderTest.class);

    /** The builder. */
    private MetaModelBuilder builder;

    /**
     * Sets the up.
     *
     * @param <X> the generic type
     * @param <T> the generic type
     * @throws Exception the exception
     */
    @Before
    public <X extends Class, T extends Object> void setUp() throws Exception
    {
        builder = new MetaModelBuilder<X, T>();
    }

    /**
     * Test construct.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void testEntityWithSingularAttribute()
    {
        X clazz = (X) SingularEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = SingularEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(SingularEntity.class, f);
        }

        try
        {
            Field managedTypeField = builder.getClass().getDeclaredField("managedType");
            if (!managedTypeField.isAccessible())
            {
                managedTypeField.setAccessible(true);
            }

            AbstractManagedType<X> managedType = assertOnManagedType(builder, managedTypeField, SingularEntity.class);
            assertOnIdAttribute(managedType);

            // on optional attribute
            log.info("Assert on optional attribute");
            Assert.assertEquals("name", managedType.getSingularAttribute("name").getName());
            Assert.assertTrue(managedType.getSingularAttribute("name").isOptional());
            Assert.assertEquals(String.class, managedType.getSingularAttribute("name").getJavaType());

            Boolean found = null;
            try
            {
                found = managedType.getSingularAttribute("name", Integer.class) != null;
                Assert.fail("should not be called");
            }
            catch (IllegalArgumentException iaex)
            {
                log.info("Assert on invalid case");
                Assert.assertNull(found);
            }

            try
            {
                found = managedType.getSingularAttribute("name", String.class) != null;
                Assert.assertNotNull(found);
                Assert.assertTrue(found);
            }
            catch (IllegalArgumentException iaex)
            {
                log.info("Error on positive case");
                Assert.fail(iaex.getMessage());
            }

            // on not optional attribute
            Assert.assertEquals("field", managedType.getSingularAttribute("field").getName());
            Assert.assertFalse(managedType.getSingularAttribute("field").isOptional());
            Assert.assertEquals(String.class, managedType.getSingularAttribute("field").getJavaType());

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
     * Test on entity with embeddable.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void testOnEntityWithEmbeddable()

    {
        X clazz = (X) SingularEntityEmbeddable.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = SingularEntityEmbeddable.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(SingularEntityEmbeddable.class, f);
        }

        MetaModelBuilder.class.getDeclaredFields();
        Field embeddableField;
        try
        {
            embeddableField = builder.getClass().getDeclaredField("embeddables");
            if (!embeddableField.isAccessible())
            {
                embeddableField.setAccessible(true);
            }
            Map<Class<?>, AbstractManagedType<?>> embeddables = ((Map<Class<?>, AbstractManagedType<?>>) embeddableField
                    .get(builder));
            Assert.assertEquals(2, embeddables.size());

            Field managedTypeField = builder.getClass().getDeclaredField("managedType");
            if (!managedTypeField.isAccessible())
            {
                managedTypeField.setAccessible(true);
            }

            AbstractManagedType<X> managedType = assertOnManagedType(builder, managedTypeField,
                    SingularEntityEmbeddable.class);

            assertOnIdAttribute(managedType);

            // assert on embeddable first attribute
            SingularAttribute embeddableAttrib = managedType.getSingularAttribute("embeddableEntity");
            assertOnEmbeddable(embeddableAttrib, EmbeddableEntity.class);
            EmbeddableType<X> embeddableType = (EmbeddableType<X>) embeddableAttrib.getType();
            Attribute<X, String> attribute = (Attribute<X, String>) embeddableType.getAttribute("embeddedField");
            assertOnEmbeddableType(EmbeddableEntity.class, attribute, embeddableType, "embeddedField", String.class);

            // assert on embeddable second attribute
            SingularAttribute embeddableAttribTwo = managedType.getSingularAttribute("embeddableEntityTwo",
                    EmbeddableEntityTwo.class);
            EmbeddableType<X> embeddableTypeTwo = (EmbeddableType<X>) embeddableAttribTwo.getType();
            assertOnEmbeddable(embeddableAttribTwo, EmbeddableEntityTwo.class);
            assertOnEmbeddableType(EmbeddableEntityTwo.class, attribute, embeddableTypeTwo, "embeddedField",
                    Float.class);

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
     * Combined test.
     *
     * @param <X> the generic type
     * @param <T> the generic type
     */
    public <X extends Class, T extends Object> void combinedTest()
    {
        X clazz = (X) SingularEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = SingularEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(SingularEntity.class, f);
        }

        clazz = (X) SingularEntityEmbeddable.class;

        builder.process(clazz);
        field = SingularEntityEmbeddable.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(SingularEntityEmbeddable.class, f);
        }
        try
        {
            Field managedTypesFields = builder.getClass().getDeclaredField("managedTypes");
            if (!managedTypesFields.isAccessible())
            {
                managedTypesFields.setAccessible(true);
            }
            Map<Class<?>, AbstractManagedType<?>> managedTypes = ((Map<Class<?>, AbstractManagedType<?>>) managedTypesFields
                    .get(builder));
            Assert.assertEquals(2, managedTypes.size());
            
            //TODO: Refactor and more assertions are required.
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
     * Assert on embeddable type.
     * 
     * @param <X>
     *            the generic type
     * @param entityClazz
     *            the entity clazz
     * @param attribute
     *            the attribute
     * @param embeddableType
     *            the embeddable type
     * @param attributeName
     *            the attribute name
     * @param attributeClazz
     *            the attribute clazz
     */
    private <X> void assertOnEmbeddableType(Class entityClazz, Attribute<X, String> attribute,
            EmbeddableType<X> embeddableType, String attributeName, Class attributeClazz)
    {
        Assert.assertEquals(entityClazz.getDeclaredFields().length, embeddableType.getAttributes().size());
        Assert.assertEquals(entityClazz, embeddableType.getJavaType());
        Attribute attributeTwo = (Attribute) embeddableType.getAttribute(attributeName);
        Assert.assertNotNull(attribute);
        Assert.assertEquals(attributeClazz, attributeTwo.getJavaType());
        Assert.assertEquals(attributeName, attributeTwo.getName());
    }

    /**
     * Assert on embeddable.
     * 
     * @param embeddableAttrib
     *            the embeddable attrib
     * @param clazz
     *            the clazz
     */
    private void assertOnEmbeddable(SingularAttribute embeddableAttrib, Class clazz)
    {
        Assert.assertNotNull(embeddableAttrib);
        Assert.assertEquals(PersistentAttributeType.EMBEDDED, embeddableAttrib.getPersistentAttributeType());
        Assert.assertEquals(PersistenceType.EMBEDDABLE, embeddableAttrib.getType().getPersistenceType());
        Assert.assertEquals(clazz, embeddableAttrib.getType().getJavaType());
    }

    /**
     * Assert on managed type.
     * 
     * @param <X>
     *            the generic type
     * @param builder
     *            the builder
     * @param managedTypeField
     *            the managed type field
     * @param clazz
     *            the clazz
     * @return the abstract managed type
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    private <X> AbstractManagedType<X> assertOnManagedType(MetaModelBuilder builder, Field managedTypeField,
            Class<?> clazz) throws IllegalAccessException
    {
        log.info("Assert on managedType");
        AbstractManagedType<X> managedType = (AbstractManagedType<X>) managedTypeField.get(builder);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(clazz, ((EntityType<X>) managedType).getBindableJavaType());
        Assert.assertEquals(BindableType.ENTITY_TYPE, ((EntityType<X>) managedType).getBindableType());
        Assert.assertEquals(PersistenceType.ENTITY, ((EntityType<X>) managedType).getPersistenceType());
        Assert.assertEquals(clazz.getSimpleName(), ((EntityType<X>) managedType).getName());
        Assert.assertEquals(clazz.getDeclaredFields().length, managedType.getSingularAttributes().size());
        return managedType;
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @Test
    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        builder = null;
    }

    /**
     * Assert on id attribute.
     * 
     * @param <X>
     *            the generic type
     * @param managedType
     *            the managed type
     */
    private <X> void assertOnIdAttribute(AbstractManagedType<X> managedType)
    {
        // assert on id attribute.
        log.info("Assert on id attribute");
        Assert.assertEquals("key", managedType.getSingularAttribute("key").getName());
        Assert.assertTrue(((AbstractIdentifiableType<X>) managedType).hasSingleIdAttribute());
        SingularAttribute<? super X, Integer> idAttribute = ((AbstractIdentifiableType<X>) managedType)
                .getId(Integer.class);
        Assert.assertNotNull(idAttribute);
        Assert.assertTrue(idAttribute.isId());
        Assert.assertFalse(idAttribute.isOptional());
        Assert.assertEquals(idAttribute.getName(), "key");
        Assert.assertEquals(Integer.class, idAttribute.getJavaType());
    }
}
