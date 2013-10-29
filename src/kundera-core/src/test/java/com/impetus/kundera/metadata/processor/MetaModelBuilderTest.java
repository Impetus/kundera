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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.InheritanceType;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.Bindable;
import javax.persistence.metamodel.Bindable.BindableType;
import javax.persistence.metamodel.CollectionAttribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.IdentifiableType;
import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;
import javax.persistence.metamodel.Type.PersistenceType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.entities.AssociationEntity;
import com.impetus.kundera.metadata.entities.AttributeOverrideSubClass;
import com.impetus.kundera.metadata.entities.CollectionTypeAssociationEntity;
import com.impetus.kundera.metadata.entities.EmbeddableEntity;
import com.impetus.kundera.metadata.entities.EmbeddableEntityTwo;
import com.impetus.kundera.metadata.entities.EmbeddedIdOwnerEntity;
import com.impetus.kundera.metadata.entities.EntityWithAttributeOverrides;
import com.impetus.kundera.metadata.entities.EntityWithIdAttributeOverride;
import com.impetus.kundera.metadata.entities.IDClassEntity;
import com.impetus.kundera.metadata.entities.IDClassOwnerEntity;
import com.impetus.kundera.metadata.entities.ListTypeAssociationEntity;
import com.impetus.kundera.metadata.entities.MapTypeAssociationEntity;
import com.impetus.kundera.metadata.entities.OToMOwnerEntity;
import com.impetus.kundera.metadata.entities.OToOOwnerEntity;
import com.impetus.kundera.metadata.entities.PluralOwnerType;
import com.impetus.kundera.metadata.entities.SetTypeAssociationEntity;
import com.impetus.kundera.metadata.entities.SingularEntity;
import com.impetus.kundera.metadata.entities.SingularEntityEmbeddable;
import com.impetus.kundera.metadata.entities.SubClassA;
import com.impetus.kundera.metadata.entities.SubClassB;
import com.impetus.kundera.metadata.entities.SubSingularEntity;
import com.impetus.kundera.metadata.entities.bi.AssociationBiEntity;
import com.impetus.kundera.metadata.entities.bi.OToOOwnerBiEntity;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractIdentifiableType;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * MetaModelBuilderTest.
 * 
 * @author vivek.mishra
 * 
 */
public class MetaModelBuilderTest
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(MetaModelBuilderTest.class);

    /** The builder. */
    @SuppressWarnings("rawtypes")
    private MetaModelBuilder builder;

    /**
     * Sets the up.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     * @throws Exception
     *             the exception
     */
    @SuppressWarnings("rawtypes")
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
            assertOnIdAttribute(managedType, "key", Integer.class);
            // illeagaltypesTest(managedType);

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
    @SuppressWarnings({ "rawtypes", "unchecked" })
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

            assertOnIdAttribute(managedType, "key", Integer.class);

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
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
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
        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertEquals(2, managedTypes.size());
    }

    /**
     * test case for 1-1 uni association.
     * 
     * @param <X>
     *            entity class
     * @param <T>
     *            field type.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public <X extends Class, T extends Object> void testOneToOneUniAssociation()
    {
        X clazz = (X) OToOOwnerEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = OToOOwnerEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(OToOOwnerEntity.class, f);
        }

        clazz = (X) AssociationEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = AssociationEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(AssociationEntity.class, f);

        }

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

        // Assertion on owner Entity
        AbstractManagedType<?> managedType = managedTypes.get(OToOOwnerEntity.class);
        Assert.assertNotNull(managedType);
        // Assert.assertEquals(OToOOwnerEntity.class.getDeclaredFields().length,
        // managedType.getAttributes().size());
        // Assert.assertEquals(OToOOwnerEntity.class.getDeclaredFields().length,
        // managedType.getDeclaredAttributes()
        // .size());

        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(OToOOwnerEntity.class), managedType
                .getAttributes().size());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(OToOOwnerEntity.class), managedType
                .getDeclaredAttributes().size());

        assertOnIdAttribute(managedType, "rowKey", byte.class);

        // asssert on association attribute.
        Attribute<? super X, ?> associationAttribute = (Attribute<? super X, ?>) managedType
                .getAttribute("association");
        Assert.assertNotNull(associationAttribute);
        Assert.assertEquals(PersistentAttributeType.ONE_TO_ONE, associationAttribute.getPersistentAttributeType());
        Assert.assertEquals(AssociationEntity.class, associationAttribute.getJavaType());
        Assert.assertEquals(true, associationAttribute.isAssociation());
        Assert.assertEquals(OToOOwnerEntity.class, associationAttribute.getDeclaringType().getJavaType());
        Assert.assertEquals("association", associationAttribute.getName());

        // Assertion on AssociationEntity.
        managedType = managedTypes.get(AssociationEntity.class);
        Assert.assertNotNull(managedType);
        // Assert.assertEquals(AssociationEntity.class.getDeclaredFields().length,
        // managedType.getAttributes().size());
        // Assert.assertEquals(AssociationEntity.class,
        // managedType.getJavaType());
        // Assert.assertEquals(AssociationEntity.class.getDeclaredFields().length,
        // managedType.getDeclaredAttributes()
        // .size());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(AssociationEntity.class), managedType
                .getAttributes().size());
        Assert.assertEquals(AssociationEntity.class, managedType.getJavaType());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(AssociationEntity.class), managedType
                .getDeclaredAttributes().size());
        assertOnIdAttribute(managedType, "assoRowKey", String.class);

    }

    /**
     * test case for 1-1 uni association.
     * 
     * @param <X>
     *            entity class
     * @param <T>
     *            field type.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public <X extends Class, T extends Object> void testOneToOneBiAssociation()
    {
        X clazz = (X) OToOOwnerBiEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = OToOOwnerBiEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(OToOOwnerBiEntity.class, f);
        }

        clazz = (X) AssociationBiEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = AssociationBiEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(AssociationBiEntity.class, f);
        }

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

        // Assertion on owner Entity
        AbstractManagedType<?> managedType = managedTypes.get(OToOOwnerBiEntity.class);
        Assert.assertNotNull(managedType);
        // Assert.assertEquals(OToOOwnerBiEntity.class.getDeclaredFields().length,
        // managedType.getAttributes().size());
        // Assert.assertEquals(OToOOwnerBiEntity.class.getDeclaredFields().length,
        // managedType.getDeclaredAttributes()
        // .size());

        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(OToOOwnerBiEntity.class), managedType
                .getAttributes().size());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(OToOOwnerBiEntity.class), managedType
                .getDeclaredAttributes().size());

        assertOnIdAttribute(managedType, "rowKey", byte.class);

        // asssert on association attribute.
        Attribute<? super X, ?> associationAttribute = (Attribute<? super X, ?>) managedType
                .getAttribute("association");
        Assert.assertNotNull(associationAttribute);
        Assert.assertEquals(PersistentAttributeType.ONE_TO_ONE, associationAttribute.getPersistentAttributeType());
        Assert.assertEquals(AssociationBiEntity.class, associationAttribute.getJavaType());
        Assert.assertEquals(true, associationAttribute.isAssociation());
        Assert.assertEquals(OToOOwnerBiEntity.class, associationAttribute.getDeclaringType().getJavaType());
        Assert.assertEquals("association", associationAttribute.getName());

        // Assertion on AssociationBiEntity.
        managedType = managedTypes.get(AssociationBiEntity.class);
        Assert.assertNotNull(managedType);
        // Assert.assertEquals(AssociationBiEntity.class.getDeclaredFields().length,
        // managedType.getAttributes().size());
        // Assert.assertEquals(AssociationBiEntity.class,
        // managedType.getJavaType());
        // Assert.assertEquals(AssociationBiEntity.class.getDeclaredFields().length,
        // managedType.getDeclaredAttributes()
        // .size());

        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(AssociationBiEntity.class), managedType
                .getAttributes().size());
        Assert.assertEquals(AssociationBiEntity.class, managedType.getJavaType());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(AssociationBiEntity.class), managedType
                .getDeclaredAttributes().size());

        assertOnIdAttribute(managedType, "assoRowKey", String.class);

        // assert on owner attribute
        Attribute<? super X, ?> ownerAttribute = (Attribute<? super X, ?>) managedType.getAttribute("owner");
        Assert.assertNotNull(ownerAttribute);
        Assert.assertEquals(PersistentAttributeType.ONE_TO_ONE, ownerAttribute.getPersistentAttributeType());
        Assert.assertEquals(OToOOwnerBiEntity.class, ownerAttribute.getJavaType());
        Assert.assertEquals(true, ownerAttribute.isAssociation());
        Assert.assertEquals(AssociationBiEntity.class, ownerAttribute.getDeclaringType().getJavaType());
        Assert.assertEquals("owner", ownerAttribute.getName());
        Assert.assertEquals(managedTypes.get(AssociationBiEntity.class), ownerAttribute.getDeclaringType());
        Assert.assertEquals(AssociationBiEntity.class, ownerAttribute.getJavaMember().getDeclaringClass());
        Assert.assertEquals(OToOOwnerBiEntity.class, ownerAttribute.getJavaType());
        Assert.assertEquals(managedTypes.get(OToOOwnerBiEntity.class),
                ((SingularAttribute<? super X, ?>) ownerAttribute).getType());

    }

    /**
     * Test on collection.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public <X extends Class, T extends Object> void testOnUniOneToManyCollection()
    {
        X clazz = (X) OToMOwnerEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = OToMOwnerEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(OToMOwnerEntity.class, f);
        }

        clazz = (X) AssociationEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = AssociationEntity.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(AssociationEntity.class, f);
        }
        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

        // Assertion on owner Entity
        AbstractManagedType<?> managedType = managedTypes.get(OToMOwnerEntity.class);
        Assert.assertNotNull(managedType);
        // Assert.assertEquals(OToMOwnerEntity.class.getDeclaredFields().length,
        // managedType.getAttributes().size());
        // Assert.assertEquals(OToMOwnerEntity.class.getDeclaredFields().length,
        // managedType.getDeclaredAttributes()
        // .size());

        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(OToMOwnerEntity.class), managedType
                .getAttributes().size());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(OToMOwnerEntity.class), managedType
                .getDeclaredAttributes().size());

        assertOnIdAttribute(managedType, "rowKey", byte.class);

        // assert on getCollection.
        CollectionAttribute<? super X, ?> collectionAttribute = (CollectionAttribute<? super X, ?>) managedType
                .getCollection("association", AssociationEntity.class);
        Assert.assertNotNull(collectionAttribute);

        // assert on getDeclaredCollection.
        CollectionAttribute<? super X, ?> declaredCollectionAttribute = (CollectionAttribute<? super X, ?>) managedType
                .getDeclaredCollection("association", AssociationEntity.class);
        Assert.assertNotNull(declaredCollectionAttribute);

        // assert on getDeclaredCollection.
        CollectionAttribute<? super X, ?> declaredCollectionAttributeParam = (CollectionAttribute<? super X, ?>) managedType
                .getDeclaredCollection("association");
        Assert.assertNotNull(declaredCollectionAttributeParam);

        // assert with invalid collection type class.
        try
        {
            collectionAttribute = null;
            collectionAttribute = (CollectionAttribute<? super X, ?>) managedType.getCollection("association",
                    AssociationBiEntity.class);
            Assert.fail();
        }
        catch (IllegalArgumentException iaex)
        {
            log.info("on invalid case with getCollection");
            Assert.assertNull(collectionAttribute);
        }

        try
        {
            collectionAttribute = null;
            collectionAttribute = (CollectionAttribute<? super X, ?>) managedType.getCollection("associationInvalid");
            Assert.fail();
        }
        catch (IllegalArgumentException iaex)
        {
            log.info("on invalid case with getCollection");
            Assert.assertNull(collectionAttribute);
        }
        // asssert on association attribute.
        Attribute<? super X, ?> associationAttribute = (Attribute<? super X, ?>) managedType
                .getAttribute("association");
        Assert.assertNotNull(associationAttribute);
        Assert.assertEquals(PersistentAttributeType.ONE_TO_MANY, associationAttribute.getPersistentAttributeType());
        Assert.assertEquals(Collection.class, associationAttribute.getJavaType());
        Assert.assertEquals(AssociationEntity.class, ((Bindable) associationAttribute).getBindableJavaType());
        Assert.assertEquals(true, associationAttribute.isAssociation());
        Assert.assertEquals(OToMOwnerEntity.class, associationAttribute.getDeclaringType().getJavaType());
        Assert.assertEquals("association", associationAttribute.getName());
    }

    /**
     * Test on list.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @SuppressWarnings("unchecked")
    @Test
    public <X extends Class, T extends Object> void testOnAllUniPlural()
    {
        X clazz = (X) PluralOwnerType.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = PluralOwnerType.class.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(PluralOwnerType.class, f);
        }

        onCollectionAssociation((X) SetTypeAssociationEntity.class);

        onCollectionAssociation((X) ListTypeAssociationEntity.class);

        onCollectionAssociation((X) MapTypeAssociationEntity.class);

        onCollectionAssociation((X) CollectionTypeAssociationEntity.class);

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(5, managedTypes.size());

        // assert on set association entity.
        AbstractManagedType<? super X> managedType = (AbstractManagedType<? super X>) managedTypes
                .get(SetTypeAssociationEntity.class);
        assertOnCollectionAttribute(managedType, "setKey", "bytes", SetTypeAssociationEntity.class);

        // assert on map association entity.
        managedType = (AbstractManagedType<? super X>) managedTypes.get(MapTypeAssociationEntity.class);
        assertOnCollectionAttribute(managedType, "mapKey", "bytes", MapTypeAssociationEntity.class);

        // assert on map association entity.
        managedType = (AbstractManagedType<? super X>) managedTypes.get(CollectionTypeAssociationEntity.class);
        assertOnCollectionAttribute(managedType, "colKey", "bytes", CollectionTypeAssociationEntity.class);

        // assert on map association entity.
        managedType = (AbstractManagedType<? super X>) managedTypes.get(ListTypeAssociationEntity.class);
        assertOnCollectionAttribute(managedType, "listKey", "bytes", ListTypeAssociationEntity.class);

        // Assert on owner class.
        managedType = (AbstractManagedType<? super X>) managedTypes.get(PluralOwnerType.class);
        assertOnOwnerTypeAttributes(managedType, "setAssocition", SetTypeAssociationEntity.class, Set.class);
        assertOnOwnerTypeAttributes(managedType, "listAssociation", ListTypeAssociationEntity.class, List.class);
        assertOnOwnerTypeAttributes(managedType, "collectionAssociation", CollectionTypeAssociationEntity.class,
                Collection.class);
        assertOnOwnerTypeAttributes(managedType, "mapAssociation", MapTypeAssociationEntity.class, Map.class);

        // Assert on set attribute class.
        assertOnSetAttribute(managedType, "setAssocition", SetTypeAssociationEntity.class, Set.class);
        // Assert on list attribute class.
        assertOnListAttribute(managedType, "listAssociation", ListTypeAssociationEntity.class, List.class);
        // Assert on map attribute class.
        assertOnMapAttribute(managedType, "mapAssociation", Integer.class, MapTypeAssociationEntity.class);

    }

    /**
     * On id class test.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void onIdClassTest()
    {
        X clazz = (X) IDClassOwnerEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(1, managedTypes.size());
        AbstractManagedType managedType = managedTypes.get(IDClassOwnerEntity.class);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(2, managedType.getAttributes().size());
        Attribute idAttribute = managedType.getAttribute("id");

        SingularAttribute idAttrib = null;
        try
        {
            idAttrib = ((IdentifiableType) managedType).getId(IDClassEntity.class);
            Assert.fail();
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNull(idAttrib);
        }
        Assert.assertEquals(2, ((IdentifiableType) managedType).getIdClassAttributes().size());
        Assert.assertNotNull(idAttribute);
        Assert.assertTrue(((SingularAttribute) idAttribute).isId());
    }

    /**
     * On embedded id test.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void onEmbeddedIdTest()
    {
        X clazz = (X) EmbeddedIdOwnerEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(1, managedTypes.size());
        AbstractManagedType managedType = managedTypes.get(clazz);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(2, managedType.getAttributes().size());
        Attribute idAttribute = managedType.getAttribute("id");

        SingularAttribute idAttrib = null;
        try
        {
            idAttrib = ((IdentifiableType) managedType).getId(EmbeddableEntity.class);
            Assert.fail();
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNull(idAttrib);
        }
        Assert.assertEquals(1, ((IdentifiableType) managedType).getIdClassAttributes().size());
        Assert.assertNotNull(idAttribute);
        Assert.assertTrue(((SingularAttribute) idAttribute).isId());
    }

    /**
     * Test mapped super class.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void testMappedSuperClass()
    {
        X clazz = (X) SubClassA.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        clazz = (X) SubClassB.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

        Assert.assertEquals(1, builder.getMappedSuperClassTypes().size());

        // on subClass A
        AbstractManagedType managedType = managedTypes.get(SubClassA.class);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(5, managedType.getAttributes().size());
        Assert.assertNotNull(managedType.getAttribute("mappedInt"));
        Assert.assertNotNull(managedType.getSingularAttribute("mappedInt", int.class));

        // on subClass A
        managedType = managedTypes.get(SubClassB.class);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(5, managedType.getAttributes().size());
        Assert.assertNotNull(managedType.getAttribute("mappedInt"));
        Assert.assertNotNull(managedType.getSingularAttribute("mappedInt", int.class));
    }
    
    /**
     * Test mapped super class.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void testAssociationMappedSuperClass()
    {
        X clazz = (X) CarPart.class;
      
        builder.process(clazz);
        
        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(0, builder.getMappedSuperClassTypes().size());
        
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        clazz = (X) CarPartResource.class;
       
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

        Assert.assertEquals(1, builder.getMappedSuperClassTypes().size());

        // on class CarPart
        AbstractManagedType managedType = managedTypes.get(CarPart.class);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(2, managedType.getAttributes().size());
    

        // on class CarPartResource
        managedType = managedTypes.get(CarPartResource.class);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(5, managedType.getAttributes().size());
       
    }
    
    /**
     * Test mapped super class.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */
    @Test
    public <X extends Class, T extends Object> void testEmbeddableForMappedSuperClass()
    {
        Field embeddableField;
        
        X clazz = (X) CarPart.class;
      
        builder.process(clazz);
        
        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(0, builder.getMappedSuperClassTypes().size());
     
        
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        clazz = (X) CarPartResource.class;
       
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

       
        Assert.assertEquals(1, builder.getMappedSuperClassTypes().size());

       
    

        // on class CarPartResource
        AbstractManagedType managedType = managedTypes.get(CarPartResource.class);
        Assert.assertNotNull(managedType);
       
        
       
        
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

         

            // assert on embeddable first attribute
            SingularAttribute embeddableAttrib = managedType.getSingularAttribute("engine");
            assertOnEmbeddable(embeddableAttrib, CarEngine.class);
            EmbeddableType<X> embeddableType = (EmbeddableType<X>) embeddableAttrib.getType();
            Attribute<X, String> attribute = (Attribute<X, String>) embeddableType.getAttribute("engineId");
            assertOnEmbeddableType(CarEngine.class, attribute, embeddableType, "engineId", String.class);

           

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


    @Test
    public <X extends Class, T extends Object> void testAttributeOverride()
    {

        X clazz = (X) AttributeOverrideSubClass.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        clazz = (X) EntityWithAttributeOverrides.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        Map<Class<?>, AbstractManagedType<?>> managedTypes = getManagedTypes();
        Assert.assertNotNull(managedTypes);
        Assert.assertEquals(2, managedTypes.size());

        Assert.assertEquals(1, builder.getMappedSuperClassTypes().size());

        // on subClass EntityWithAttributeOverrides
        AbstractManagedType managedType = managedTypes.get(EntityWithAttributeOverrides.class);
        Assert.assertNotNull(managedType);
        Assert.assertEquals(5, managedType.getAttributes().size());
        Assert.assertNotNull(managedType.getAttribute("mappedInt"));

        String jpaName1 = ((AbstractAttribute<X, T>) managedType.getAttribute("mappedInt")).getJPAColumnName();

        String jpaName2 = ((AbstractAttribute<X, T>) managedType.getAttribute("mappedFloat")).getJPAColumnName();

        Assert.assertEquals("MAPPED_INT", jpaName1);
        Assert.assertEquals("MAPPED_FLOAT", jpaName2);

        Assert.assertNotNull(managedType.getSingularAttribute("mappedInt", int.class));

        // on subClass AttributeOverrideSubClass
        managedType = managedTypes.get(AttributeOverrideSubClass.class);

        jpaName1 = ((AbstractAttribute<X, T>) managedType.getAttribute("mappedInt")).getJPAColumnName();

        jpaName2 = ((AbstractAttribute<X, T>) managedType.getAttribute("mappedFloat")).getJPAColumnName();

        Assert.assertEquals("MAPPED_INT", jpaName1);
        Assert.assertEquals("mappedFloat", jpaName2);

        Assert.assertNotNull(managedType);
        Assert.assertEquals(5, managedType.getAttributes().size());
        Assert.assertNotNull(managedType.getAttribute("mappedInt"));
        Assert.assertNotNull(managedType.getSingularAttribute("mappedInt", int.class));

        clazz = (X) EntityWithIdAttributeOverride.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }

        managedType = managedTypes.get(EntityWithIdAttributeOverride.class);

        jpaName1 = ((AbstractAttribute<X, T>) managedType.getAttribute("id")).getJPAColumnName();

        jpaName2 = ((AbstractAttribute<X, T>) managedType.getAttribute("mappedFloat")).getJPAColumnName();

        String jpaName3 = ((AbstractAttribute<X, T>) managedType.getAttribute("mappedInt")).getJPAColumnName();

        Assert.assertEquals("ID_ATTRIB", jpaName1);
        Assert.assertEquals("MAPPED_FLOAT", jpaName2);
        Assert.assertEquals("mappedInt", jpaName3);

    }

    @Test
    public <X extends Class, T extends Object> void testInheritenceStrategy()
    {
        X clazz = (X) SubSingularEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        Field[] field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }
        
        EntityType<X> managedType = (EntityType<X>) builder.getManagedTypes().get(SubSingularEntity.class);
        
        InheritanceType type = ((AbstractManagedType<X>)managedType).getInheritenceType();
        
        Assert.assertEquals(InheritanceType.TABLE_PER_CLASS, type);
        
        
         clazz = (X) SingularEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
        }
        
    }

    /**
     * Assert on owner type attributes.
     * 
     * @param managedType
     *            the managed type
     * @param fieldName
     *            the field name
     * @param fieldClazz
     *            the field clazz
     * @param javaClazz
     *            the java clazz
     */
    private void assertOnOwnerTypeAttributes(AbstractManagedType managedType, String fieldName, Class fieldClazz,
            Class javaClazz)
    {
        Assert.assertNotNull(managedType);
        Assert.assertNotNull(managedType.getPluralAttributes());
        Assert.assertEquals(4, managedType.getPluralAttributes().size());
        Assert.assertNotNull(managedType.getAttribute(fieldName));
        Assert.assertEquals(javaClazz, managedType.getAttribute(fieldName).getJavaType());
        Assert.assertEquals(fieldClazz, ((PluralAttribute) managedType.getAttribute(fieldName)).getElementType()
                .getJavaType());
        Assert.assertEquals(BindableType.PLURAL_ATTRIBUTE,
                ((PluralAttribute) managedType.getAttribute(fieldName)).getBindableType());
        Assert.assertNotNull(((PluralAttribute) managedType.getAttribute(fieldName)).getJavaMember());
        Assert.assertNotNull(fieldName, ((PluralAttribute) managedType.getAttribute(fieldName)).getJavaMember()
                .getName());

    }

    /**
     * Assert on collection attribute.
     * 
     * @param <X>
     *            the generic type
     * @param managedType
     *            the managed type
     * @param id
     *            the id
     * @param otherAttribute
     *            the other attribute
     * @param clazz
     *            the clazz
     */
    private <X> void assertOnCollectionAttribute(AbstractManagedType<? super X> managedType, String id,
            String otherAttribute, Class clazz)
    {
        Assert.assertNotNull(managedType);
        Assert.assertEquals(clazz, managedType.getJavaType());
        SingularAttribute<? super X, String> rowId = (SingularAttribute<? super X, String>) managedType
                .getDeclaredSingularAttribute(id);
        Assert.assertNotNull(rowId);
        Assert.assertTrue(rowId.isId());
        Assert.assertEquals(String.class, rowId.getBindableJavaType());
        // other attribute
        SingularAttribute<? super X, String> byteAttribute = (SingularAttribute<? super X, String>) managedType
                .getDeclaredAttribute(otherAttribute);
        Assert.assertNotNull(byteAttribute);
        Assert.assertFalse(byteAttribute.isId());
        Assert.assertEquals(byte[].class, byteAttribute.getBindableJavaType());

    }

    /**
     * On collection association.
     * 
     * @param <X>
     *            the generic type
     * @param clazz
     *            the clazz
     */
    private <X extends Class> void onCollectionAssociation(X clazz)
    {
        Field[] field;
        // X clazz = (X) SetTypeAssociationEntity.class;
        // MetaModelBuilder builder = new MetaModelBuilder<X, T>();
        builder.process(clazz);
        field = clazz.getDeclaredFields();
        for (Field f : field)
        {
            builder.construct(clazz, f);
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <X> void assertOnEmbeddableType(Class entityClazz, Attribute<X, String> attribute,
            EmbeddableType<X> embeddableType, String attributeName, Class attributeClazz)
    {
        // Assert.assertEquals(entityClazz.getDeclaredFields().length,
        // embeddableType.getAttributes().size());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(entityClazz), embeddableType.getAttributes()
                .size());
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
        // Assert.assertEquals(clazz.getDeclaredFields().length,
        // managedType.getSingularAttributes().size());
        Assert.assertEquals(KunderaCoreUtils.countNonSyntheticFields(clazz), managedType.getSingularAttributes()
                .size());
        return managedType;
    }

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
     * @param key
     *            the key
     * @param clazz
     *            the clazz
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <X> void assertOnIdAttribute(AbstractManagedType<X> managedType, String key, Class clazz)
    {
        // assert on id attribute.
        log.info("Assert on id attribute");
        Assert.assertEquals(key, managedType.getSingularAttribute(key).getName());
        Assert.assertTrue(((AbstractIdentifiableType<X>) managedType).hasSingleIdAttribute());

        SingularAttribute<? super X, Integer> idAttribute = ((AbstractIdentifiableType<X>) managedType).getId(clazz);

        Assert.assertNotNull(idAttribute);
        Assert.assertTrue(idAttribute.isId());
        Assert.assertFalse(idAttribute.isOptional());
        Assert.assertEquals(idAttribute.getName(), key);
        Assert.assertEquals(clazz, idAttribute.getJavaType());

        SingularAttribute<? super X, Integer> idAttributeDeclared = ((AbstractIdentifiableType<X>) managedType)
                .getDeclaredId(clazz);

        Assert.assertNotNull(idAttributeDeclared);
        Assert.assertTrue(idAttributeDeclared.isId());
        Assert.assertFalse(idAttributeDeclared.isOptional());
        Assert.assertEquals(idAttributeDeclared.getName(), key);
        Assert.assertEquals(clazz, idAttributeDeclared.getJavaType());

        Type<?> idType = ((AbstractIdentifiableType<X>) managedType).getIdType();
        Assert.assertNotNull(idType);

        IdentifiableType<? super X> idIdentifiableType = ((AbstractIdentifiableType<X>) managedType).getSupertype();
        Assert.assertNull(idIdentifiableType);

    }

    /**
     * Assert on set attributes.
     * 
     * @param managedType
     *            the managed type
     * @param fieldName
     *            the field name
     * @param fieldClazz
     *            the field clazz
     * @param javaClazz
     *            the java clazz
     */
    private <X> void assertOnSetAttribute(AbstractManagedType<? super X> managedType, String fieldName,
            Class fieldClazz, Class javaClazz)
    {
        SetAttribute<? super X, ?> rowSetId = (SetAttribute<? super X, ?>) managedType.getSet(fieldName);
        Assert.assertNotNull(rowSetId);
        Assert.assertEquals(rowSetId.getName(), fieldName);
        Assert.assertEquals(Set.class, rowSetId.getJavaType());

        SetAttribute<? super X, ?> rowDeclaredId = (SetAttribute<? super X, ?>) managedType.getDeclaredSet(fieldName);
        Assert.assertNotNull(rowDeclaredId);
        Assert.assertEquals(rowDeclaredId.getName(), fieldName);
        Assert.assertEquals(Set.class, rowDeclaredId.getJavaType());

        SetAttribute<? super X, ?> rowClassSetId = (SetAttribute<? super X, ?>) managedType.getSet(fieldName,
                fieldClazz);
        Assert.assertNotNull(rowClassSetId);
        Assert.assertEquals(rowClassSetId.getName(), fieldName);
        Assert.assertEquals(Set.class, rowClassSetId.getJavaType());

        SetAttribute<? super X, ?> rowClassDeclaredId = (SetAttribute<? super X, ?>) managedType.getDeclaredSet(
                fieldName, fieldClazz);
        Assert.assertNotNull(rowClassSetId);
        Assert.assertEquals(rowClassDeclaredId.getName(), fieldName);
        Assert.assertEquals(Set.class, rowClassDeclaredId.getJavaType());

    }

    /**
     * Assert on List attributes.
     * 
     * @param managedType
     *            the managed type
     * @param fieldName
     *            the field name
     * @param fieldClazz
     *            the field clazz
     * @param javaClazz
     *            the java clazz
     */
    private <X> void assertOnListAttribute(AbstractManagedType<? super X> managedType, String fieldName,
            Class fieldClazz, Class javaClazz)
    {
        ListAttribute<? super X, ?> rowListId = (ListAttribute<? super X, ?>) managedType.getList(fieldName);
        Assert.assertNotNull(rowListId);
        Assert.assertEquals(rowListId.getName(), fieldName);
        Assert.assertEquals(List.class, rowListId.getJavaType());

        ListAttribute<? super X, ?> listDeclaredId = (ListAttribute<? super X, ?>) managedType
                .getDeclaredList(fieldName);
        Assert.assertNotNull(listDeclaredId);
        Assert.assertEquals(listDeclaredId.getName(), fieldName);
        Assert.assertEquals(List.class, listDeclaredId.getJavaType());

        ListAttribute<? super X, ?> listClassSetId = (ListAttribute<? super X, ?>) managedType.getList(fieldName,
                fieldClazz);
        Assert.assertNotNull(listClassSetId);
        Assert.assertEquals(listClassSetId.getName(), fieldName);
        Assert.assertEquals(List.class, listClassSetId.getJavaType());

        ListAttribute<? super X, ?> listClassDeclaredId = (ListAttribute<? super X, ?>) managedType.getDeclaredList(
                fieldName, fieldClazz);
        Assert.assertNotNull(listClassDeclaredId);
        Assert.assertEquals(listClassDeclaredId.getName(), fieldName);
        Assert.assertEquals(List.class, listClassDeclaredId.getJavaType());

    }

    /**
     * Assert on Map attributes.
     * 
     * @param managedType
     *            the managed type
     * @param fieldName
     *            the field name
     * @param fieldClazz
     *            the field clazz
     * @param javaClazz
     *            the java clazz
     */
    private <X> void assertOnMapAttribute(AbstractManagedType<? super X> managedType, String fieldName,
            Class fieldClazz, Class javaClazz)
    {
        MapAttribute<? super X, ?, ?> mapSetId = (MapAttribute<? super X, ?, ?>) managedType.getMap(fieldName);
        Assert.assertNotNull(mapSetId);
        Assert.assertEquals(mapSetId.getName(), fieldName);
        Assert.assertEquals(Map.class, mapSetId.getJavaType());

        MapAttribute<? super X, ?, ?> mapDeclaredId = (MapAttribute<? super X, ?, ?>) managedType
                .getDeclaredMap(fieldName);
        Assert.assertNotNull(mapDeclaredId);
        Assert.assertEquals(mapDeclaredId.getName(), fieldName);
        Assert.assertEquals(Map.class, mapDeclaredId.getJavaType());

        /*
         * MapAttribute<? super X, ?, ?> rowClassSetId = (MapAttribute<? super
         * X, ?, ?>) managedType .getMap(fieldName,int.class,String.class);
         * Assert.assertNotNull(rowClassSetId);
         * 
         * 
         * MapAttribute<? super X, ?, ?> rowClassDeclaredId = (MapAttribute<?
         * super X, ?, ?> ) managedType .getDeclaredMap(fieldName, fieldClazz,
         * javaClazz); Assert.assertNotNull(rowClassSetId);
         */

    }

    /**
     * Gets the managed types.
     * 
     * @return the managed types
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Map<Class<?>, AbstractManagedType<?>> getManagedTypes()
    {
        try
        {
            Field managedTypesFields = builder.getClass().getDeclaredField("managedTypes");
            if (!managedTypesFields.isAccessible())
            {
                managedTypesFields.setAccessible(true);
            }

            return ((Map<Class<?>, AbstractManagedType<?>>) managedTypesFields.get(builder));
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
        return null;
    }

    /**
     * On embedded id test.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     */

    private <X> void illeagaltypesTest(AbstractManagedType<X> managedType)
    {
        try
        {

            SingularAttribute<? super X, Double> idNullAttribute = ((AbstractIdentifiableType<X>) managedType)
                    .getId(Double.class);

        }

        catch (IllegalArgumentException e)
        {
            Assert.fail(e.getMessage());
        }

    }

}
