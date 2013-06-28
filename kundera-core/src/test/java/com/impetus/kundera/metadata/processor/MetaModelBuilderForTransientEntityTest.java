/**
 * 
 */
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.entities.EmbeddableTransientEntity;
import com.impetus.kundera.metadata.entities.TransientEntity;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MetaModelBuilderForTransientEntityTest
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(MetaModelBuilderForTransientEntityTest.class);

    /** The builder. */
    @SuppressWarnings("rawtypes")
    private MetaModelBuilder builder;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public <X extends Class, T extends Object> void setUp() throws Exception
    {
        builder = new MetaModelBuilder<X, T>();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        builder = null;
    }

    @Test
    public <X extends Class, T extends Object> void testEntityWithTransientAttribute()
    {

        X clazz = (X) TransientEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = TransientEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(TransientEntity.class, f);
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
            Assert.assertEquals(1, embeddables.size());

            Field managedTypeField = builder.getClass().getDeclaredField("managedType");
            if (!managedTypeField.isAccessible())
            {
                managedTypeField.setAccessible(true);
            }

            AbstractManagedType<X> managedType = assertOnManagedType(builder, managedTypeField, TransientEntity.class);

            // assert on embeddable first attribute
            SingularAttribute embeddableAttrib = managedType.getSingularAttribute("embeddableTransientField");
            assertOnEmbeddable(embeddableAttrib, EmbeddableTransientEntity.class);
            EmbeddableType<X> embeddableType = (EmbeddableType<X>) embeddableAttrib.getType();
            Attribute<X, String> attribute = (Attribute<X, String>) embeddableType.getAttribute("embeddedField");
            assertOnEmbeddableType(EmbeddableTransientEntity.class, attribute, embeddableType, "embeddedField",
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
        Assert.assertNotSame(clazz.getDeclaredFields().length, managedType.getSingularAttributes().size());
        Assert.assertEquals(3, managedType.getSingularAttributes().size());
        return managedType;
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <X> void assertOnEmbeddableType(Class entityClazz, Attribute<X, String> attribute,
            EmbeddableType<X> embeddableType, String attributeName, Class attributeClazz)
    {
        Assert.assertNotSame(entityClazz.getDeclaredFields().length, embeddableType.getAttributes().size());
        Assert.assertEquals(1, embeddableType.getAttributes().size());
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void assertOnEmbeddable(SingularAttribute embeddableAttrib, Class clazz)
    {
        Assert.assertNotNull(embeddableAttrib);
        Assert.assertEquals(PersistentAttributeType.EMBEDDED, embeddableAttrib.getPersistentAttributeType());
        Assert.assertEquals(PersistenceType.EMBEDDABLE, embeddableAttrib.getType().getPersistenceType());
        Assert.assertEquals(clazz, embeddableAttrib.getType().getJavaType());
    }
}
