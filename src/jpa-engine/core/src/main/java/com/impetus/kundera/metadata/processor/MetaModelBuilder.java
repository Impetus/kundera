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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javassist.Modifier;

import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Transient;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.MappedSuperclassType;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;
import javax.persistence.metamodel.Type.PersistenceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.model.attributes.DefaultCollectionAttribute;
import com.impetus.kundera.metadata.model.attributes.DefaultListAttribute;
import com.impetus.kundera.metadata.model.attributes.DefaultMapAttribute;
import com.impetus.kundera.metadata.model.attributes.DefaultSetAttribute;
import com.impetus.kundera.metadata.model.attributes.DefaultSingularAttribute;
import com.impetus.kundera.metadata.model.type.AbstractIdentifiableType;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.metadata.model.type.DefaultBasicType;
import com.impetus.kundera.metadata.model.type.DefaultEmbeddableType;
import com.impetus.kundera.metadata.model.type.DefaultEntityType;
import com.impetus.kundera.metadata.model.type.DefaultMappedSuperClass;

/**
 * The Class MetaModelBuilder.
 * 
 * @param <X>
 *            the generic managed type
 * @param <T>
 *            the generic attribute type
 * 
 *            TODO : Handle of {@link MappedSuperclass}, {@link IdClass}
 * 
 * @author vivek.mishra
 */
public final class MetaModelBuilder<X, T>
{
    /** The Constant log. */
    private static final Logger LOG = LoggerFactory.getLogger(MetaModelBuilder.class);

    /** The managed type. */
    private AbstractManagedType<X> managedType;

    /** The managed types. */
    private Map<Class<?>, EntityType<?>> managedTypes = new HashMap<Class<?>, EntityType<?>>();

    /** The mapped super class types. */
    private Map<Class<?>, ManagedType<?>> mappedSuperClassTypes = new HashMap<Class<?>, ManagedType<?>>();

    /** The embeddables. */
    private Map<Class<?>, AbstractManagedType<?>> embeddables = new HashMap<Class<?>, AbstractManagedType<?>>();

    /**
     * Process.
     * 
     * @param clazz
     *            the clazz
     */
    public void process(Class<X> clazz)
    {
        this.managedType = processInternal(clazz, false);

        // TODO: this.managedType has to be removed.
        // Need a validation that TypeBuilder class must be same as
        // MetaModelBuilder class.

        // TODO::: To handle association cases, need to check from managedType
        // only and populate the same.

    }

    /**
     * Adds the embeddables.
     * 
     * @param clazz
     *            the clazz
     * @param embeddable
     *            the embeddable
     */
    void addEmbeddables(Class<?> clazz, AbstractManagedType<?> embeddable)
    {
        embeddables.put(clazz, embeddable);
    }

    /**
     * Construct.
     * 
     * @param clazz
     *            the clazz
     * @param attribute
     *            the attribute
     */
    void construct(Class<X> clazz, Field attribute)
    {
        TypeBuilder<X> typeBuilder = new TypeBuilder<X>(attribute);
        typeBuilder.build((AbstractManagedType) managedTypes.get(clazz), attribute.getType());
    }

    /**
     * The Class TypeBuilder.
     * 
     * @param <X>
     *            the generic type
     */
    private class TypeBuilder<X>
    {

        /** The attribute. */
        private Field attribute;

        /** The persistent attribute type. */
        private PersistentAttributeType persistentAttribType;

        /**
         * Instantiates a new type builder.
         * 
         * @param attribute
         *            the attribute
         */
        TypeBuilder(Field attribute)
        {
            this.attribute = attribute;
        }

        /**
         * Instantiates a new type builder.
         * 
         * @param attribute
         *            the attribute
         * @param persistentAttribType
         *            the persistent attrib type
         */
        TypeBuilder(Field attribute, PersistentAttributeType persistentAttribType)
        {
            this.attribute = attribute;
            this.persistentAttribType = persistentAttribType;
        }

        /**
         * Builds the type.
         * 
         * @param <T>
         *            the generic type
         * @param attribType
         *            the attrib type
         * @return the type
         */
        <T> Type<T> buildType(Class<T> attribType)
        {
            // Only in case of Map attribute attribute will be null;
            PersistentAttributeType attributeType = attribute != null ? MetaModelBuilder
                    .getPersistentAttributeType(attribute) : persistentAttribType;
            switch (attributeType)
            {
            case BASIC:
                return new DefaultBasicType<T>(attribType);
            case EMBEDDED:
                return processOnEmbeddables(attribType);

            case ELEMENT_COLLECTION:
                if (attribute != null && Collection.class.isAssignableFrom(attribType))
                {
                    java.lang.reflect.Type[] argument = ((ParameterizedType) attribute.getGenericType())
                            .getActualTypeArguments();

                    return processOnEmbeddables(getTypedClass(argument[0]));
                }
                else if (attribute != null && Map.class.isAssignableFrom(attribType))
                {
                    java.lang.reflect.Type[] argument = ((ParameterizedType) attribute.getGenericType())
                            .getActualTypeArguments();
                    processOnEmbeddables(getTypedClass(argument[0]));
                    return processOnEmbeddables(getTypedClass(argument[1]));
                }
                else
                {
                    LOG.warn("Cannot process for : " + attribute
                            + " as it is not a collection but annotated with @ElementCollection");
                }
            default:
                if (!(managedTypes.get(attribType) != null))
                {
                    // get Generic type from attribute and then pass it.
                    if (attribute != null && isPluralAttribute(attribute))
                    {
                        java.lang.reflect.Type[] arguments = ((ParameterizedType) attribute.getGenericType())
                                .getActualTypeArguments();
                        if (arguments != null && arguments.length == 1)
                        {
                            attribType = (Class<T>) getTypedClass(arguments[0]);
                        }
                        else if (arguments != null && arguments.length > 1)
                        {
                            attribType = (Class<T>) getTypedClass(arguments[1]);
                        }
                    }

                    // If generic typed class is managed entity.
                    if (managedTypes.get(attribType) == null)
                    {

                        if (attribType.isAnnotationPresent(Entity.class))
                        {
                            EntityType<T> entityType = new DefaultEntityType<T>((Class<T>) attribType,
                                    PersistenceType.ENTITY, (AbstractIdentifiableType) getType(
                                            attribType.getSuperclass(), false));
                            managedTypes.put(attribType, entityType);
                        }
                        else
                        {
                            return new DefaultBasicType<T>(attribType);
                        }
                    }
                }
                return (Type<T>) managedTypes.get(attribType);
            }
        }

        /**
         * Process on embeddables.
         * 
         * @param <T>
         *            the generic type
         * @param attribType
         *            the attrib type
         * @return the abstract managed type
         */
        private AbstractManagedType processOnEmbeddables(Class attribType)
        {
            // Check if this embeddable type is already present in
            // collection of MetaModelBuider.
            AbstractManagedType<T> embeddableType = null;

            PersistenceType persistenceType = PersistenceType.BASIC;
            Annotation embeddableAnnotation = attribType.getAnnotation(Embeddable.class);
            if (embeddableAnnotation != null)
            {
                persistenceType = PersistenceType.EMBEDDABLE;
            }

            if (!embeddables.containsKey(attribType))
            {
                embeddableType = new DefaultEmbeddableType<T>(attribType, persistenceType, null);

                if (attribute != null)
                {
                    Field[] embeddedFields = attribType.getDeclaredFields();
                    for (Field f : embeddedFields)
                    {
                        if (isNonTransient(f))
                        {
                            new TypeBuilder<T>(f).build(embeddableType, f.getType());
                        }
                    }
                }
                addEmbeddables(attribType, embeddableType);
            }
            else
            {
                embeddableType = (AbstractManagedType<T>) embeddables.get(attribType);
            }

            onPostProcess(embeddableType);

            return embeddableType;
        }

        /**
         * Checks for constraint on embeddabletype and accordingly set it on
         * managedType
         * 
         * @param embeddableType
         */
        private void onPostProcess(AbstractManagedType<T> embeddableType)
        {
            try
            {
                if (managedType != null)
                {
                    
                    Field managedTypeField = managedType.getClass().getSuperclass().getSuperclass()
                            .getDeclaredField("hasValidationConstraints");

                    if (!managedTypeField.isAccessible())
                    {
                        managedTypeField.setAccessible(true);
                    }

                    if (embeddableType.hasValidationConstraints())
                    {
                        managedTypeField.set(managedType, true);

                    }

                }
            }
            catch (Exception e)
            {
                LOG.error("Error setting Contstraint for managed type, Caused by: {}", e);
                throw new MetamodelLoaderException("Error setting Contstraint for managed type" + e.getMessage());
            }

        }

        /**
         * Builds the.
         * 
         * @param managedType
         *            the managed type
         * @param attributeType
         *            the attribute type
         */
        void build(AbstractManagedType managedType, Class attributeType)
        {
            new AttributeBuilder(attribute, managedType, buildType(attributeType)).build();
        }

        /**
         * The Class AttributeBuilder.
         * 
         * @param <X>
         *            the generic type
         * @param <T>
         *            the generic type
         */
        private class AttributeBuilder<X, T>
        {

            /** The attribute. */
            private Field attribute;

            /** The attribute type. */
            private Type<T> attributeType;

            /** The managed type. */
            private AbstractManagedType<X> managedType;

            /**
             * Instantiates a new attribute builder.
             * 
             * @param attribute
             *            the attribute
             * @param managedType
             *            the managed type
             * @param attributeType
             *            the attribute type
             */
            public AttributeBuilder(Field attribute, AbstractManagedType<X> managedType, Type<T> attributeType)
            {
                this.attribute = attribute;
                this.managedType = managedType;
                this.attributeType = attributeType;
            }

            /**
             * Builds the.
             * 
             * @param <K>
             *            the key type
             * @param <V>
             *            the value type
             */
            public <K, V> void build()
            {
                if (isNonTransient(attribute))
                {
                    if (!managedType.hasLobAttribute() && attribute.getAnnotation(Lob.class) != null)
                        managedType.setHasLobAttribute(true);

                    if (isPluralAttribute(attribute))
                    {
                        PluralAttribute<X, ?, ?> pluralAttribute = null;
                        if (attribute.getType().equals(java.util.Collection.class))
                        {
                            pluralAttribute = new DefaultCollectionAttribute<X, T>(attributeType, attribute.getName(),
                                    getAttributeType(), managedType, attribute,
                                    (Class<java.util.Collection<T>>) attribute.getType());
                        }
                        else if (attribute.getType().equals(java.util.List.class))
                        {
                            pluralAttribute = new DefaultListAttribute<X, T>(attributeType, attribute.getName(),
                                    getAttributeType(), managedType, attribute, (Class<List<T>>) attribute.getType());
                        }
                        else if (attribute.getType().equals(java.util.Set.class))
                        {
                            pluralAttribute = new DefaultSetAttribute<X, T>(attributeType, attribute.getName(),
                                    getAttributeType(), managedType, attribute, (Class<Set<T>>) attribute.getType());
                        }
                        else if (attribute.getType().equals(java.util.Map.class))
                        {
                            java.lang.reflect.Type[] arguments = ((ParameterizedType) attribute.getGenericType())
                                    .getActualTypeArguments();

                            Type keyType = new TypeBuilder<X>(null, getPersistentAttributeType(attribute))
                                    .buildType(getTypedClass(arguments[0]));
                            pluralAttribute = new DefaultMapAttribute(attributeType, attribute.getName(),
                                    getAttributeType(), managedType, attribute, (Class<Map<T, ?>>) attribute.getType(),
                                    keyType);
                        }
                        ((AbstractManagedType<X>) managedType).addPluralAttribute(attribute.getName(), pluralAttribute);
                    }
                    else
                    {
                        SingularAttribute<X, T> singularAttribute = new DefaultSingularAttribute(attribute.getName(),
                                getAttributeType(), attribute, attributeType, managedType, checkId(attribute));
                        ((AbstractManagedType<X>) managedType).addSingularAttribute(attribute.getName(),
                                singularAttribute);

                        if (checkSimpleId(attribute) && checkIdClass(managedType.getJavaType()))

                        {
                            IdClass anno = managedType.getJavaType().getAnnotation(IdClass.class);
                            AbstractManagedType superType = onSuperType(anno.value(), true);
                            onDeclaredFields(anno.value(), superType);
                            ((AbstractIdentifiableType<X>) managedType).addIdAttribute(singularAttribute, true,
                                    superType.getDeclaredSingularAttributes());
                        }
                        else if (checkEmbeddedId(attribute))
                        {
                            AbstractManagedType superType = onSuperType(attribute.getType(), false);
                            checkEmbeddable(superType.getJavaType(), attribute.getName());
                            ((AbstractIdentifiableType<X>) managedType).addIdAttribute(singularAttribute, true,
                                    superType.getDeclaredSingularAttributes());

                        }
                        else if (checkSimpleId(attribute))
                        {
                            ((AbstractIdentifiableType<X>) managedType).addIdAttribute(singularAttribute, false, null);
                        }
                    }
                }
            }

            /**
             * Validates that super type must be embeddable.
             * 
             * @param superType
             * @throws MetamodelLoaderException
             *             exception.
             */
            private void checkEmbeddable(Class superType, String fieldname)
            {
                // check validity.
                if (superType != null && !superType.isAnnotationPresent(Embeddable.class))
                {
                    throw new MetamodelLoaderException("Field: " + fieldname
                            + " is annotated with @EmbeddedId but corresponding class:" + superType
                            + " is not an @Embeddable entity");
                }
            }

            private AbstractManagedType onSuperType(Class clazz, boolean isIdClass)
            {
                AbstractManagedType superType = getType(clazz, isIdClass);

                isValidId(superType);

                return superType;
            }

            /**
             * Check id.
             * 
             * @param member
             *            the member
             * @return true, if successful
             */
            boolean checkId(Field member)
            {
                return checkSimpleId(member) || checkEmbeddedId(member);
            }

            /**
             * Check simple id.
             * 
             * @param member
             *            the member
             * @return true, if successful
             */
            boolean checkSimpleId(Field member)
            {
                return member.isAnnotationPresent(Id.class);
            }

            /**
             * Check id class.
             * 
             * @param member
             *            the member
             * @return true, if successful
             */
            boolean checkIdClass(Class member)
            {
                return member.isAnnotationPresent(IdClass.class);
            }

            /**
             * Check embedded id.
             * 
             * @param member
             *            the member
             * @return true, if successful
             */
            boolean checkEmbeddedId(Field member)
            {
                return member.isAnnotationPresent(EmbeddedId.class);
            }

            /**
             * Gets the attribute type.
             * 
             * @return the attribute type
             */
            PersistentAttributeType getAttributeType()
            {
                return MetaModelBuilder.getPersistentAttributeType(attribute);
            }
        }

        private void isValidId(AbstractManagedType superType)
        {
            if (superType == null)
            {
                throw new MetamodelLoaderException("field : " + attribute.getName()
                        + " is either annotated with @EmbeddedId or class:" + managedType.getJavaType()
                        + " is annotated with @Idclass, but enclosed class is not having any member");
            }
        }

    }

    /**
     * Gets the managed types.
     * 
     * @return the managedTypes
     */
    public Map<Class<?>, EntityType<?>> getManagedTypes()
    {
        return managedTypes;
    }

    /**
     * Gets the embeddables.
     * 
     * @return the embeddables
     */
    public Map<Class<?>, AbstractManagedType<?>> getEmbeddables()
    {
        return embeddables;
    }

    /**
     * @return the mappedSuperClassTypes
     */
    public Map<Class<?>, ManagedType<?>> getMappedSuperClassTypes()
    {
        return mappedSuperClassTypes;
    }

    /**
     * Returns true, if attribute belongs plural hierarchy.
     * 
     * @param attribute
     *            the attribute
     * @return true, if attribute belongs plural hierarchy. else false.
     */
    private boolean isPluralAttribute(Field attribute)
    {
        return attribute.getType().equals(Collection.class) || attribute.getType().equals(Set.class)
                || attribute.getType().equals(List.class) || attribute.getType().equals(Map.class);
    }

    /**
     * Gets the typed class.
     * 
     * @param type
     *            the type
     * @return the typed class
     */
    private Class<?> getTypedClass(java.lang.reflect.Type type)
    {
        if (type instanceof Class)
        {
            return ((Class) type);
        }
        else if (type instanceof ParameterizedType)
        {
            java.lang.reflect.Type rawParamterizedType = ((ParameterizedType) type).getRawType();
            return getTypedClass(rawParamterizedType);
        }
        else if (type instanceof TypeVariable)
        {
            java.lang.reflect.Type upperBound = ((TypeVariable) type).getBounds()[0];
            return getTypedClass(upperBound);
        }

        throw new IllegalArgumentException("Error while finding generic class for :" + type);
    }

    /**
     * Gets the persistent attribute type.
     * 
     * @param member
     *            the member
     * @return the persistent attribute type
     */
    static PersistentAttributeType getPersistentAttributeType(Field member)
    {
        PersistentAttributeType attributeType = PersistentAttributeType.BASIC;
        if (member.isAnnotationPresent(ElementCollection.class))
        {
            attributeType = PersistentAttributeType.ELEMENT_COLLECTION;
        }
        else if (member.isAnnotationPresent(OneToMany.class))
        {
            attributeType = PersistentAttributeType.ONE_TO_MANY;
        }
        else if (member.isAnnotationPresent(ManyToMany.class))
        {
            attributeType = PersistentAttributeType.MANY_TO_MANY;
        }
        else if (member.isAnnotationPresent(ManyToOne.class))
        {
            attributeType = PersistentAttributeType.MANY_TO_ONE;
        }
        else if (member.isAnnotationPresent(OneToOne.class))
        {
            attributeType = PersistentAttributeType.ONE_TO_ONE;
        }
        else if (member.isAnnotationPresent(Embedded.class))
        {
            attributeType = PersistentAttributeType.EMBEDDED;
        }

        return attributeType;

    }

    /**
     * Builds the managed type.
     * 
     * @param <X>
     *            the generic type
     * @param clazz
     *            the clazz
     * @return the abstract managed type
     */
    private <X> AbstractManagedType<X> buildManagedType(Class<X> clazz, boolean isIdClass)
    {
        AbstractManagedType<X> managedType = null;
        if (clazz.isAnnotationPresent(Embeddable.class))
        {

            validate(clazz, true);
            if (!embeddables.containsKey(clazz))
            {
                managedType = new DefaultEmbeddableType<X>(clazz, PersistenceType.EMBEDDABLE, getType(
                        clazz.getSuperclass(), isIdClass));
                onDeclaredFields(clazz, managedType);
                embeddables.put(clazz, managedType);
            }
            else
            {
                managedType = (AbstractManagedType<X>) embeddables.get(clazz);
            }

        }
        else if (clazz.isAnnotationPresent(MappedSuperclass.class))
        {

            validate(clazz, false);
            // if (!mappedSuperClassTypes.containsKey(clazz))
            // {
            managedType = new DefaultMappedSuperClass<X>(clazz, PersistenceType.MAPPED_SUPERCLASS,
                    (AbstractIdentifiableType) getType(clazz.getSuperclass(), isIdClass));
            onDeclaredFields(clazz, managedType);
            mappedSuperClassTypes.put(clazz, (MappedSuperclassType<?>) managedType);
            // }
            // else
            // {
            // managedType = (AbstractManagedType<X>)
            // mappedSuperClassTypes.get(clazz);
            // }
        }
        else if (clazz.isAnnotationPresent(Entity.class) || isIdClass)
        {
            if (!managedTypes.containsKey(clazz))
            {
                managedType = new DefaultEntityType<X>(clazz, PersistenceType.ENTITY,
                        (AbstractIdentifiableType) getType(clazz.getSuperclass(), isIdClass));
                // in case of @IdClass, it is a temporary managed type.
                if (!isIdClass)
                {
                    managedTypes.put(clazz, (EntityType<?>) managedType);
                }
            }
            else
            {
                managedType = (AbstractManagedType<X>) managedTypes.get(clazz);
            }
        }

        return managedType;
    }

    /**
     * Gets the super type.
     * 
     * @param clazz
     *            the clazz
     * @return the super type
     */
    private AbstractManagedType getType(Class clazz, boolean isIdClass)
    {
        if (clazz != null && !clazz.equals(Object.class))
        {
            return processInternal(clazz, isIdClass);
        }
        return null;
    }

    /**
     * Validate.
     * 
     * @param <X>
     *            the generic type
     * @param clazz
     *            the clazz
     * @param isEmbeddable
     *            the is embeddable
     */
    private <X> void validate(Class<X> clazz, boolean isEmbeddable)
    {
        final String mappedSuperClazzErrMsg = "Class:" + clazz
                + "is annotated with @MappedSuperClass and @Entity not allowed";
        final String embeddableClazzErrMsg = "Class:" + clazz + "is annotated with @Embeddable and @Entity not allowed";
        if (clazz.isAnnotationPresent(Entity.class))
        {
            throw new MetamodelLoaderException(isEmbeddable ? embeddableClazzErrMsg : mappedSuperClazzErrMsg);
        }
    }

    /**
     * On declared fields.
     * 
     * @param <X>
     *            the generic type
     * @param clazz
     *            the clazz
     * @param managedType
     *            the managed type
     */
    private <X> void onDeclaredFields(Class<X> clazz, AbstractManagedType<X> managedType)
    {
        Field[] embeddedFields = clazz.getDeclaredFields();
        for (Field f : embeddedFields)
        {
            if (isNonTransient(f))
            {
                new TypeBuilder<T>(f).build(managedType, f.getType());
            }
        }
    }

    /**
     * Process.
     * 
     * @param clazz
     *            the clazz
     * @return the abstract managed type
     */
    private AbstractManagedType<X> processInternal(Class<X> clazz, boolean isIdClass)
    {
        if (managedTypes.get(clazz) == null)
        {
            return buildManagedType(clazz, isIdClass);
        }
        else
        {
            return (AbstractManagedType<X>) managedTypes.get(clazz);
        }
    }

    /**
     * @return
     */
    private boolean isNonTransient(Field attribute)
    {
        return attribute != null && !Modifier.isStatic(attribute.getModifiers())
                && !Modifier.isTransient(attribute.getModifiers()) && !attribute.isAnnotationPresent(Transient.class);
    }

}
