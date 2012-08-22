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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.metamodel.Attribute.PersistentAttributeType;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type;
import javax.persistence.metamodel.Type.PersistenceType;

import org.hibernate.mapping.Collection;

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
 * @param <X> the generic managed type
 * @param <T> the generic attribute type
 * 
 *TODO : Handle of {@link MappedSuperclass}, {@link IdClass} 
 * @author vivek.mishra
 */
public final class MetaModelBuilder<X, T>
{

    
    /** The managed type. */
    private AbstractManagedType<X> managedType;

    /**
     * Instantiates a new meta model builder.
     *
     * @param clazz the clazz
     */
    public MetaModelBuilder(Class<X> clazz)
    {
        this.managedType = buildManagedType(clazz);
    }

    /**
     * Construct.
     *
     * @param clazz the clazz
     * @param attribute the attribute
     */
    void construct(Class<X> clazz, Field attribute)
    {
        TypeBuilder<X> typeBuilder = new TypeBuilder<X>(attribute);
        typeBuilder.build(managedType);
    }

    /**
     * The Class TypeBuilder.
     *
     * @param <X> the generic type
     */
    private static class TypeBuilder<X>
    {

        /** The attribute. */
        private Field attribute;

        /**
         * Instantiates a new type builder.
         *
         * @param attribute the attribute
         */
        TypeBuilder(Field attribute)
        {
            this.attribute = attribute;
        }

        /**
         * Builds the type.
         *
         * @param <T> the generic type
         * @return the type
         */
        public <T> Type<T> buildType()
        {
            PersistentAttributeType attributeType = MetaModelBuilder.getPersistentAttributeType(attribute);
            switch (attributeType)
            {
            case BASIC:
                return new DefaultBasicType<T>((Class<T>) attribute.getType());
            case EMBEDDED:
                return (Type<T>) new DefaultEmbeddableType<T>((Class<T>) attribute.getType(),
                        PersistenceType.EMBEDDABLE, null);
            case ELEMENT_COLLECTION:
                return (Type<T>) new DefaultEmbeddableType<T>((Class<T>) attribute.getType(),
                        PersistenceType.EMBEDDABLE, null);
            default:
                return (Type<T>) new DefaultEntityType<T>((Class<T>) attribute.getType(), PersistenceType.ENTITY, null);
            }

            // TODO: Throw an error.
        }

        /**
         * Builds the.
         *
         * @param managedType the managed type
         */
        void build(AbstractManagedType<X> managedType)
        {
            // AbstractManagedType<X> managedType = buildManagedType(clazz);
            new AttributeBuilder(attribute, managedType, buildType()).build();
        }

        /**
         * The Class AttributeBuilder.
         *
         * @param <X> the generic type
         * @param <T> the generic type
         */
        private static class AttributeBuilder<X, T>
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
             * @param attribute the attribute
             * @param managedType the managed type
             * @param attributeType the attribute type
             */
            public AttributeBuilder(Field attribute, AbstractManagedType<X> managedType, Type<T> attributeType)
            {
                this.attribute = attribute;
                this.managedType = managedType;
                this.attributeType = attributeType;
            }

            /**
             * Builds the.
             */
            public void build()
            {
                if (attribute.getType().equals(Collection.class) || attribute.getType().equals(Set.class)
                        || attribute.getType().equals(List.class) || attribute.getType().equals(Map.class))
                {

                }
                else
                {
                    SingularAttribute<X, T> singularAttribute = new DefaultSingularAttribute(attribute.getName(), getAttributeType(), attribute,
                            attributeType, managedType, checkId(attribute));
                    ((AbstractManagedType<X>) managedType).addSingularAttribute(attribute.getName(),
                            singularAttribute);
                    if(checkSimpleId(attribute))
                    {
                        ((AbstractIdentifiableType<X>)managedType).addIdAttribute(singularAttribute, false, null);
                    } else if(checkIdClass(attribute))
                    {
                        //TODO:: need to handle this.
                    }
                }

            }

            boolean checkId(Field member)
            {
                return checkSimpleId(member) || checkIdClass(member) || checkEmbeddedId(member);
            }
            boolean checkSimpleId(Field member)
            {
                return member.isAnnotationPresent(Id.class);
            }
            boolean checkIdClass(Field member)
            {
                return member.isAnnotationPresent(IdClass.class);
            }
            
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
    }

    /**
     * Gets the persistent attribute type.
     *
     * @param member the member
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

    // TODO: still i need to identify for how to set super clazz type.
    // This has to be revisited.

    /**
     * Builds the managed type.
     *
     * @param <X> the generic type
     * @param clazz the clazz
     * @return the abstract managed type
     */
    private static <X> AbstractManagedType<X> buildManagedType(Class<X> clazz)
    {
        AbstractManagedType<X> managedType = null;
        if (clazz.isAnnotationPresent(Embeddable.class))
        {
            // TODO: still i need to identify for how to set super clazz type.
            // This has to be revisited.
            managedType = new DefaultEmbeddableType<X>(clazz, PersistenceType.EMBEDDABLE, null);
        }
        else if (clazz.isAnnotationPresent(MappedSuperclass.class))
        {
            // TODO: still i need to identify for how to set super clazz type.
            // This has to be revisited.
            managedType = new DefaultMappedSuperClass<X>(clazz, PersistenceType.MAPPED_SUPERCLASS, null);
        }
        else
        {
            // TODO: still i need to identify for how to set super clazz type.
            // This has to be revisited.
            managedType = new DefaultEntityType<X>(clazz, PersistenceType.ENTITY, null);
        }

        return managedType;
    }

}
