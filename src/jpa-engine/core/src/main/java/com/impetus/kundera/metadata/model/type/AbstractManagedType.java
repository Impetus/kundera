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
package com.impetus.kundera.metadata.model.type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Bindable;
import javax.persistence.metamodel.CollectionAttribute;
import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.MapAttribute;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.PluralAttribute.CollectionType;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;

import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.annotation.DefaultEntityAnnotationProcessor;
import com.impetus.kundera.metadata.model.annotation.EntityAnnotationProcessor;

/**
 * Implementation for <code>ManagedType</code> interface.
 * 
 * @author vivek.mishra
 * @param <X>
 *            the generic entity type.
 */
public abstract class AbstractManagedType<X> extends AbstractType<X> implements ManagedType<X>
{

    /** The super clazz type. */
    private ManagedType<? super X> superClazzType;

    /** The declared singluar attribs. */
    private Map<String, SingularAttribute<X, ?>> declaredSingluarAttribs;

    /** The declared plural attributes. */
    private Map<String, PluralAttribute<X, ?, ?>> declaredPluralAttributes;

    /** The Constant validJPAAnnotations. */
    private static final List<Class<? extends Annotation>> validJPAAnnotations = Arrays.asList(
            AttributeOverrides.class, AttributeOverride.class);

    /** The column bindings. */
    private Map<String, Column> columnBindings = new ConcurrentHashMap<String, Column>();

    /** The model. */
    private InheritanceModel model;

    /** The entity annotation processor. */
    private EntityAnnotationProcessor entityAnnotationProcessor;

    /** The sub managed types. */
    private List<ManagedType<X>> subManagedTypes = new ArrayList<ManagedType<X>>();

    /** The has lob attribute. */
    private boolean hasLobAttribute;

    /**
     * Checks for lob attribute.
     * 
     * @return true, if successful
     */
    public boolean hasLobAttribute()
    {
        return hasLobAttribute;
    }

    /**
     * Sets the checks for lob attribute.
     * 
     * @param hasLobAttribute
     *            the new checks for lob attribute
     */
    public void setHasLobAttribute(boolean hasLobAttribute)
    {
        this.hasLobAttribute = hasLobAttribute;
    }

    /** Whether a managed type has validation constraints. */
    protected boolean hasValidationConstraints = false;

    /** Whether a managed type has embeddable attribute. */
    private boolean hasEmbeddableAttribute;

    /**
     * Super constructor with arguments.
     * 
     * @param clazz
     *            parameterised class.
     * @param persistenceType
     *            persistenceType.
     * @param superClazzType
     *            the super clazz type
     */
    AbstractManagedType(Class<X> clazz, javax.persistence.metamodel.Type.PersistenceType persistenceType,
            ManagedType<? super X> superClazzType)
    {
        super(clazz, persistenceType);
        this.superClazzType = superClazzType;
        bindTypeAnnotations();
        if (this.superClazzType != null)
        {
            ((AbstractManagedType<? super X>) this.superClazzType).addSubManagedType(this);
        }
        this.model = buildInheritenceModel();
        entityAnnotationProcessor = new DefaultEntityAnnotationProcessor(clazz);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getAttributes()
     */
    @Override
    public Set<Attribute<? super X, ?>> getAttributes()
    {
        Set<Attribute<? super X, ?>> attributes = new HashSet<Attribute<? super X, ?>>();

        Set<Attribute<X, ?>> declaredAttribs = getDeclaredAttributes();
        if (declaredAttribs != null)
        {
            attributes.addAll(declaredAttribs);
        }
        if (superClazzType != null)
        {
            attributes.addAll(superClazzType.getAttributes());
        }

        return attributes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getDeclaredAttributes()
     */
    @Override
    public Set<Attribute<X, ?>> getDeclaredAttributes()
    {
        Set<Attribute<X, ?>> attributes = new HashSet<Attribute<X, ?>>();

        if (declaredSingluarAttribs != null)
        {
            attributes.addAll(declaredSingluarAttribs.values());
        }
        if (declaredPluralAttributes != null)
        {
            attributes.addAll(declaredPluralAttributes.values());
        }

        return attributes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getSingularAttribute(java.lang
     * .String, java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<? super X, Y> getSingularAttribute(String paramString, Class<Y> paramClass)
    {
        SingularAttribute<? super X, Y> attribute = getDeclaredSingularAttribute(paramString, paramClass, false);
        if (superClazzType != null && attribute == null)
        {
            return superClazzType.getSingularAttribute(paramString, paramClass);
        }
        checkForValid(paramString, attribute);
        return attribute;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredSingularAttribute(
     * java.lang.String, java.lang.Class)
     */
    @Override
    public <Y> SingularAttribute<X, Y> getDeclaredSingularAttribute(String paramString, Class<Y> paramClass)
    {
        return getDeclaredSingularAttribute(paramString, paramClass, true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getSingularAttributes()
     */
    @SuppressWarnings("unchecked")
    @Override
    public Set<SingularAttribute<? super X, ?>> getSingularAttributes()
    {
        Set<SingularAttribute<? super X, ?>> singularAttrib = new HashSet<SingularAttribute<? super X, ?>>();

        if (superClazzType != null)
        {
            Set parentAttrib = superClazzType.getSingularAttributes();

            if (parentAttrib != null)
            {
                singularAttrib.addAll(parentAttrib);
            }
        }

        Set<SingularAttribute<X, ?>> declaredAttribSet = getDeclaredSingularAttributes();

        if (declaredAttribSet != null)
        {
            singularAttrib.addAll(declaredAttribSet);
        }

        return singularAttrib;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredSingularAttributes()
     */
    @Override
    public Set<SingularAttribute<X, ?>> getDeclaredSingularAttributes()
    {
        Set<SingularAttribute<X, ?>> declaredAttribSet = null;
        if (declaredSingluarAttribs != null)
        {
            declaredAttribSet = new HashSet<SingularAttribute<X, ?>>();
            declaredAttribSet.addAll(declaredSingluarAttribs.values());
        }
        return declaredAttribSet;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getCollection(java.lang.String,
     * java.lang.Class)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <E> CollectionAttribute<? super X, E> getCollection(String paramName, Class<E> paramClass)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckCollectionAttribute(declaredAttrib, paramClass))
        {
            return (CollectionAttribute<X, E>) declaredAttrib;
        }

        PluralAttribute<? super X, ?, ?> superAttrib = getPluralAttriute(paramName);

        if (onCheckCollectionAttribute(superAttrib, paramClass))
        {
            return (CollectionAttribute<? super X, E>) superAttrib;

        }
        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName
                        + " , type:" + paramClass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredCollection(java.lang
     * .String, java.lang.Class)
     */
    @Override
    public <E> CollectionAttribute<X, E> getDeclaredCollection(String paramString, Class<E> paramClass)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramString);

        if (onCheckCollectionAttribute(declaredAttrib, paramClass))
        {
            return (CollectionAttribute<X, E>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramString
                        + " , type:" + paramClass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getSet(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <E> SetAttribute<? super X, E> getSet(String paramName, Class<E> paramClass)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckSetAttribute(declaredAttrib, paramClass))
        {
            return (SetAttribute<X, E>) declaredAttrib;
        }

        PluralAttribute<? super X, ?, ?> superAttrib = getPluralAttriute(paramName);

        if (onCheckSetAttribute(superAttrib, paramClass))
        {
            return (SetAttribute<? super X, E>) superAttrib;

        }
        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName
                        + " , type:" + paramClass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredSet(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <E> SetAttribute<X, E> getDeclaredSet(String paramName, Class<E> paramClass)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckSetAttribute(declaredAttrib, paramClass))
        {
            return (SetAttribute<X, E>) declaredAttrib;
        }
        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName
                        + " , type:" + paramClass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getList(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <E> ListAttribute<? super X, E> getList(String paramName, Class<E> paramClass)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckListAttribute(declaredAttrib, paramClass))
        {
            return (ListAttribute<X, E>) declaredAttrib;
        }

        PluralAttribute<? super X, ?, ?> superAttrib = getPluralAttriute(paramName);

        if (onCheckListAttribute(superAttrib, paramClass))
        {
            return (ListAttribute<? super X, E>) superAttrib;

        }
        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName
                        + " , type:" + paramClass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredList(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <E> ListAttribute<X, E> getDeclaredList(String paramName, Class<E> paramClass)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckListAttribute(declaredAttrib, paramClass))
        {
            return (ListAttribute<X, E>) declaredAttrib;
        }
        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName
                        + " , type:" + paramClass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getMap(java.lang.String,
     * java.lang.Class, java.lang.Class)
     */
    @Override
    public <K, V> MapAttribute<? super X, K, V> getMap(String paramName, Class<K> keyClazz, Class<V> valueClazz)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckMapAttribute(declaredAttrib, valueClazz))
        {
            if (valueClazz != null && valueClazz.equals(((MapAttribute<X, K, V>) declaredAttrib).getKeyJavaType()))
            {
                return (MapAttribute<X, K, V>) declaredAttrib;

            }
        }

        PluralAttribute<? super X, ?, ?> superAttrib = getPluralAttriute(paramName);

        if (onCheckMapAttribute(superAttrib, valueClazz))
        {
            if (valueClazz != null && valueClazz.equals(((MapAttribute<? super X, K, V>) superAttrib).getKeyJavaType()))
            {
                return (MapAttribute<? super X, K, V>) superAttrib;

            }
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed MapAttribute type, for name:"
                        + paramName + " , value type:" + valueClazz + "key tpye:" + keyClazz);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredMap(java.lang.String,
     * java.lang.Class, java.lang.Class)
     */
    @Override
    public <K, V> MapAttribute<X, K, V> getDeclaredMap(String paramName, Class<K> keyClazz, Class<V> valueClazz)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (onCheckMapAttribute(declaredAttrib, valueClazz))
        {
            if (valueClazz != null && valueClazz.equals(((MapAttribute<X, K, V>) declaredAttrib).getKeyJavaType()))
            {
                return (MapAttribute<X, K, V>) declaredAttrib;

            }
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed MapAttribute type, for name:"
                        + paramName + " , value type:" + valueClazz + "key tpye:" + keyClazz);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getPluralAttributes()
     */
    @Override
    public Set<PluralAttribute<? super X, ?, ?>> getPluralAttributes()
    {
        Set<PluralAttribute<? super X, ?, ?>> pluralAttributes = new HashSet<PluralAttribute<? super X, ?, ?>>();
        Set<PluralAttribute<X, ?, ?>> declaredAttribSet = getDeclaredPluralAttributes();
        if (declaredAttribSet != null)
        {
            pluralAttributes.addAll(declaredAttribSet);
        }

        if (superClazzType != null)
        {
            pluralAttributes.addAll(superClazzType.getPluralAttributes());
        }

        return pluralAttributes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredPluralAttributes()
     */
    @Override
    public Set<PluralAttribute<X, ?, ?>> getDeclaredPluralAttributes()
    {
        Set<PluralAttribute<X, ?, ?>> declaredAttribSet = null;

        if (declaredPluralAttributes != null)
        {
            declaredAttribSet = new HashSet<PluralAttribute<X, ?, ?>>();
            declaredAttribSet.addAll(declaredPluralAttributes.values());
        }
        return declaredAttribSet;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getAttribute(java.lang.String)
     */
    @Override
    public Attribute<? super X, ?> getAttribute(String paramName)
    {
        Attribute<? super X, ?> attribute = getDeclaredAttribute(paramName, false);
        if (attribute == null && superClazzType != null)
        {
            attribute = superClazzType.getAttribute(paramName);
        }

        checkForValid(paramName, attribute);
        return attribute;

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredAttribute(java.lang
     * .String)
     */
    @Override
    public Attribute<X, ?> getDeclaredAttribute(String paramName)
    {
        Attribute<X, ?> attribute = getDeclaredAttribute(paramName, true);
        return attribute;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getSingularAttribute(java.lang
     * .String)
     */
    @Override
    public SingularAttribute<? super X, ?> getSingularAttribute(String paramString)
    {
        SingularAttribute<? super X, ?> attribute = getSingularAttribute(paramString, true);
        return attribute;
    }

    /**
     * Gets the singular attribute.
     * 
     * @param paramString
     *            the param string
     * @param checkValidity
     *            the check validity
     * @return the singular attribute
     */
    private SingularAttribute<? super X, ?> getSingularAttribute(String paramString, boolean checkValidity)
    {
        SingularAttribute<? super X, ?> attribute = getDeclaredSingularAttribute(paramString, false);

        try
        {
            if (attribute == null && superClazzType != null)
            {
                attribute = superClazzType.getSingularAttribute(paramString);
            }
        }
        catch (IllegalArgumentException iaex)
        {
            attribute = null;
            onValidity(paramString, checkValidity, attribute);
        }
        onValidity(paramString, checkValidity, attribute);
        return attribute;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredSingularAttribute(
     * java.lang.String)
     */
    @Override
    public SingularAttribute<X, ?> getDeclaredSingularAttribute(String paramString)
    {
        SingularAttribute<X, ?> attribute = getDeclaredSingularAttribute(paramString, true);

        return attribute;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getCollection(java.lang.String)
     */
    @Override
    public CollectionAttribute<? super X, ?> getCollection(String paramName)
    {
        PluralAttribute<? super X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isCollectionAttribute(declaredAttrib))
        {
            return (CollectionAttribute<X, ?>) declaredAttrib;
        }

        declaredAttrib = getPluralAttriute(paramName);

        if (isCollectionAttribute(declaredAttrib))
        {
            return (CollectionAttribute<? super X, ?>) declaredAttrib;

        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredCollection(java.lang
     * .String)
     */
    @Override
    public CollectionAttribute<X, ?> getDeclaredCollection(String paramName)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isCollectionAttribute(declaredAttrib))
        {
            return (CollectionAttribute<X, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getSet(java.lang.String)
     */
    @Override
    public SetAttribute<? super X, ?> getSet(String paramName)
    {
        PluralAttribute<? super X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isSetAttribute(declaredAttrib))
        {
            return (SetAttribute<X, ?>) declaredAttrib;
        }

        declaredAttrib = getPluralAttriute(paramName);

        if (isSetAttribute(declaredAttrib))
        {
            return (SetAttribute<X, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredSet(java.lang.String)
     */
    @Override
    public SetAttribute<X, ?> getDeclaredSet(String paramName)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isSetAttribute(declaredAttrib))
        {
            return (SetAttribute<X, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getList(java.lang.String)
     */
    @Override
    public ListAttribute<? super X, ?> getList(String paramName)
    {
        PluralAttribute<? super X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isListAttribute(declaredAttrib))
        {
            return (ListAttribute<X, ?>) declaredAttrib;
        }

        declaredAttrib = getPluralAttriute(paramName);

        if (isListAttribute(declaredAttrib))
        {
            return (ListAttribute<X, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredList(java.lang.String)
     */
    @Override
    public ListAttribute<X, ?> getDeclaredList(String paramName)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isListAttribute(declaredAttrib))
        {
            return (ListAttribute<X, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.metamodel.ManagedType#getMap(java.lang.String)
     */
    @Override
    public MapAttribute<? super X, ?, ?> getMap(String paramName)
    {
        PluralAttribute<? super X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isMapAttribute(declaredAttrib))
        {
            return (MapAttribute<X, ?, ?>) declaredAttrib;
        }

        declaredAttrib = getPluralAttriute(paramName);

        if (isMapAttribute(declaredAttrib))
        {
            return (MapAttribute<X, ?, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.metamodel.ManagedType#getDeclaredMap(java.lang.String)
     */
    @Override
    public MapAttribute<X, ?, ?> getDeclaredMap(String paramName)
    {
        PluralAttribute<X, ?, ?> declaredAttrib = getDeclaredPluralAttribute(paramName);

        if (isMapAttribute(declaredAttrib))
        {
            return (MapAttribute<X, ?, ?>) declaredAttrib;
        }

        throw new IllegalArgumentException(
                "attribute of the given name and type is not present in the managed type, for name:" + paramName);
    }

    /**
     * Gets the super clazz type.
     * 
     * @return the super clazz type
     */
    ManagedType<? super X> getSuperClazzType()
    {
        return superClazzType;
    }

    /**
     * Adds the singular attribute.
     * 
     * @param attributeName
     *            the attribute name
     * @param attribute
     *            the attribute
     */
    public void addSingularAttribute(String attributeName, SingularAttribute<X, ?> attribute)
    {
        if (declaredSingluarAttribs == null)
        {
            declaredSingluarAttribs = new HashMap<String, SingularAttribute<X, ?>>();
        }

        declaredSingluarAttribs.put(attributeName, attribute);

        onValidateAttributeConstraints((Field) attribute.getJavaMember());
        onEmbeddableAttribute((Field) attribute.getJavaMember());
    }

    /**
     * Adds the plural attribute.
     * 
     * @param attributeName
     *            the attribute name
     * @param attribute
     *            the attribute
     */
    public void addPluralAttribute(String attributeName, PluralAttribute<X, ?, ?> attribute)
    {
        if (declaredPluralAttributes == null)
        {
            declaredPluralAttributes = new HashMap<String, PluralAttribute<X, ?, ?>>();
        }

        declaredPluralAttributes.put(attributeName, attribute);

        onValidateAttributeConstraints((Field) attribute.getJavaMember());
        onEmbeddableAttribute((Field) attribute.getJavaMember());

    }

    /**
     * Gets the attribute binding.
     * 
     * @param attribute
     *            the attribute
     * @return the attribute binding
     */
    public Column getAttributeBinding(Field attribute)
    {
        return columnBindings.get(attribute.getName());
    }

    /**
     * Adds the sub managed type.
     * 
     * @param inheritedType
     *            the inherited type
     */
    private void addSubManagedType(ManagedType inheritedType)
    {

        if (Modifier.isAbstract(this.getJavaType().getModifiers()))
        {
            subManagedTypes.add(inheritedType);
        }
    }

    /**
     * Gets the sub managed type.
     * 
     * @return the sub managed type
     */
    public List<ManagedType<X>> getSubManagedType()
    {
        return subManagedTypes;
    }

    /**
     * Gets the declared plural attribute.
     * 
     * @param paramName
     *            the param name
     * @return the declared plural attribute
     */
    private PluralAttribute<X, ?, ?> getDeclaredPluralAttribute(String paramName)
    {
        return declaredPluralAttributes != null ? declaredPluralAttributes.get(paramName) : null;
    }

    /**
     * Gets the plural attriute.
     * 
     * @param paramName
     *            the param name
     * @return the plural attriute
     */
    private PluralAttribute<? super X, ?, ?> getPluralAttriute(String paramName)
    {
        if (superClazzType != null)
        {
            return ((AbstractManagedType<? super X>) superClazzType).getDeclaredPluralAttribute(paramName);
        }
        return null;
    }

    /**
     * On check collection attribute.
     * 
     * @param <E>
     *            the element type
     * @param pluralAttribute
     *            the plural attribute
     * @param paramClass
     *            the param class
     * @return true, if successful
     */
    private <E> boolean onCheckCollectionAttribute(PluralAttribute<? super X, ?, ?> pluralAttribute, Class<E> paramClass)
    {
        if (pluralAttribute != null)
        {
            if (isCollectionAttribute(pluralAttribute) && isBindable(pluralAttribute, paramClass))
            {
                return true;
            }

        }

        return false;
    }

    /**
     * On check set attribute.
     * 
     * @param <E>
     *            the element type
     * @param pluralAttribute
     *            the plural attribute
     * @param paramClass
     *            the param class
     * @return true, if successful
     */
    private <E> boolean onCheckSetAttribute(PluralAttribute<? super X, ?, ?> pluralAttribute, Class<E> paramClass)
    {
        if (pluralAttribute != null)
        {
            if (isSetAttribute(pluralAttribute) && isBindable(pluralAttribute, paramClass))
            {
                return true;
            }

        }

        return false;
    }

    /**
     * On check list attribute.
     * 
     * @param <E>
     *            the element type
     * @param pluralAttribute
     *            the plural attribute
     * @param paramClass
     *            the param class
     * @return true, if successful
     */
    private <E> boolean onCheckListAttribute(PluralAttribute<? super X, ?, ?> pluralAttribute, Class<E> paramClass)
    {
        if (pluralAttribute != null)
        {
            if (isListAttribute(pluralAttribute) && isBindable(pluralAttribute, paramClass))
            {
                return true;
            }

        }

        return false;
    }

    /**
     * On check map attribute.
     * 
     * @param <V>
     *            the value type
     * @param pluralAttribute
     *            the plural attribute
     * @param valueClazz
     *            the value clazz
     * @return true, if successful
     */
    private <V> boolean onCheckMapAttribute(PluralAttribute<? super X, ?, ?> pluralAttribute, Class<V> valueClazz)
    {
        if (pluralAttribute != null)
        {
            if (isMapAttribute(pluralAttribute) && isBindable(pluralAttribute, valueClazz))
            {
                return true;
            }

        }

        return false;
    }

    /**
     * Checks if is collection attribute.
     * 
     * @param attribute
     *            the attribute
     * @return true, if is collection attribute
     */
    private boolean isCollectionAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.COLLECTION);
    }

    /**
     * Checks if is list attribute.
     * 
     * @param attribute
     *            the attribute
     * @return true, if is list attribute
     */
    private boolean isListAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.LIST);
    }

    /**
     * Checks if is sets the attribute.
     * 
     * @param attribute
     *            the attribute
     * @return true, if is sets the attribute
     */
    private boolean isSetAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.SET);
    }

    /**
     * Checks if is map attribute.
     * 
     * @param attribute
     *            the attribute
     * @return true, if is map attribute
     */
    private boolean isMapAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.MAP);
    }

    /**
     * Checks if is bindable.
     * 
     * @param <E>
     *            the element type
     * @param attribute
     *            the attribute
     * @param elementType
     *            the element type
     * @return true, if is bindable
     */
    private <E> boolean isBindable(Bindable<?> attribute, Class<E> elementType)
    {
        return attribute != null && attribute.getBindableJavaType().equals(elementType);
    }

    /**
     * Check for valid.
     * 
     * @param paramName
     *            the param name
     * @param attribute
     *            the attribute
     */
    private void checkForValid(String paramName, Attribute<? super X, ?> attribute)
    {
        if (attribute == null)
        {
            throw new IllegalArgumentException(
                    "attribute of the given name and type is not present in the managed type, for name:" + paramName);

        }
    }

    /**
     * Gets the declared attribute.
     * 
     * @param paramName
     *            the param name
     * @param checkValidity
     *            the check validity
     * @return the declared attribute
     */
    private Attribute<X, ?> getDeclaredAttribute(String paramName, boolean checkValidity)
    {
        Attribute<X, ?> attribute = (Attribute<X, ?>) getSingularAttribute(paramName, false);

        if (attribute == null)
        {
            attribute = (Attribute<X, ?>) getDeclaredPluralAttribute(paramName);
        }

        if (checkValidity)
        {
            checkForValid(paramName, attribute);
        }
        return attribute;
    }

    /**
     * Returns declared singular attribute.
     * 
     * @param <Y>
     *            the generic type
     * @param paramString
     *            the param string
     * @param paramClass
     *            the param class
     * @param checkValidity
     *            the check validity
     * @return the declared singular attribute
     */
    private <Y> SingularAttribute<X, Y> getDeclaredSingularAttribute(String paramString, Class<Y> paramClass,
            boolean checkValidity)
    {
        SingularAttribute<X, ?> declaredAttrib = declaredSingluarAttribs.get(paramString);

        if (declaredAttrib != null && declaredAttrib.getBindableJavaType().equals(paramClass))
        {
            return (SingularAttribute<X, Y>) declaredAttrib;
        }

        if (checkValidity)
        {
            throw new IllegalArgumentException(
                    "attribute of the given name and type is not present in the managed type, for name:" + paramString
                            + " , type:" + paramClass);
        }
        return null;
    }

    /**
     * Returns declared singular attribute.
     * 
     * @param paramString
     *            the param string
     * @param checkValidity
     *            the check validity
     * @return the declared singular attribute
     */
    private SingularAttribute<X, ?> getDeclaredSingularAttribute(String paramString, boolean checkValidity)
    {
        SingularAttribute<X, ?> attribute = null;
        if (declaredSingluarAttribs != null)
        {
            attribute = declaredSingluarAttribs.get(paramString);
        }

        if (checkValidity)
        {
            checkForValid(paramString, attribute);
        }
        return attribute;
    }

    /**
     * On validity check.
     * 
     * @param paramString
     *            the param string
     * @param checkValidity
     *            the check validity
     * @param attribute
     *            the attribute
     */
    private void onValidity(String paramString, boolean checkValidity, SingularAttribute<? super X, ?> attribute)
    {
        if (checkValidity)
        {
            checkForValid(paramString, attribute);

        }
    }

    /**
     * Bind type annotations.
     */
    private void bindTypeAnnotations()
    {
        // TODO:: need to check @Embeddable attributes as well!

        // TODO:: need to Handle association override in
        // RelationMetadataProcessor.

        for (Class<? extends Annotation> ann : validJPAAnnotations)
        {
            if (getJavaType().isAnnotationPresent(ann))
            {
                checkForValid();

                Annotation annotation = getJavaType().getAnnotation(ann);

                if (ann.isAssignableFrom(AttributeOverride.class))
                {
                    bindAttribute(annotation);
                }
                else if (ann.isAssignableFrom(AttributeOverrides.class))
                {

                    AttributeOverride[] attribAnns = ((AttributeOverrides) annotation).value();

                    for (AttributeOverride attribOverann : attribAnns)
                    {
                        bindAttribute(attribOverann);
                    }

                }

            }
        }
    }

    /**
     * Bind attribute.
     * 
     * @param annotation
     *            the annotation
     */
    private void bindAttribute(Annotation annotation)
    {
        String fieldname = ((AttributeOverride) annotation).name();
        Column column = ((AttributeOverride) annotation).column();
        ((AbstractManagedType) this.superClazzType).columnBindings.put(fieldname, column);
    }

    /**
     * Check for valid.
     */
    private void checkForValid()
    {
        if (this.superClazzType == null)
        {
            throw new IllegalArgumentException(
                    "@AttributeOverride and @AttributeOverrides are only applicable if super class is @MappedSuperClass");
        }
    }

    /**
     * Build inheritance model.
     * 
     * @return inheritance model instance.
     */
    private InheritanceModel buildInheritenceModel()
    {
        InheritanceModel model = null;

        if (superClazzType != null)
        {
            // means there is a super class
            // scan for inheritence model.

            if (superClazzType.getPersistenceType().equals(PersistenceType.ENTITY)
                    && superClazzType.getJavaType().isAnnotationPresent(Inheritance.class))
            {
                Inheritance inheritenceAnn = superClazzType.getJavaType().getAnnotation(Inheritance.class);

                InheritanceType strategyType = inheritenceAnn.strategy();

                String descriminator = null;
                String descriminatorValue = null;
                String tableName = null;
                String schemaName = null;
                tableName = superClazzType.getJavaType().getSimpleName();

                if (superClazzType.getJavaType().isAnnotationPresent(Table.class))
                {
                    tableName = superClazzType.getJavaType().getAnnotation(Table.class).name();
                    schemaName = superClazzType.getJavaType().getAnnotation(Table.class).schema();
                }

                model = onStrategyType(model, strategyType, descriminator, descriminatorValue, tableName, schemaName);

            }

        }

        return model;
    }

    /**
     * On strategy type.
     * 
     * @param model
     *            the model
     * @param strategyType
     *            the strategy type
     * @param descriminator
     *            the descriminator
     * @param descriminatorValue
     *            the descriminator value
     * @param tableName
     *            the table name
     * @param schemaName
     *            the schema name
     * @return the inheritance model
     */
    private InheritanceModel onStrategyType(InheritanceModel model, InheritanceType strategyType, String descriminator,
            String descriminatorValue, String tableName, String schemaName)
    {
        switch (strategyType)
        {
        case SINGLE_TABLE:

            // if single table

            if (superClazzType.getJavaType().isAnnotationPresent(DiscriminatorColumn.class))
            {
                descriminator = superClazzType.getJavaType().getAnnotation(DiscriminatorColumn.class).name();
                descriminatorValue = getJavaType().getAnnotation(DiscriminatorValue.class).value();
            }

            model = new InheritanceModel(InheritanceType.SINGLE_TABLE, descriminator, descriminatorValue, tableName,
                    schemaName);

            break;

        case JOINED:

            // if join table
            // TODOO: PRIMARY KEY JOIN COLUMN
            model = new InheritanceModel(InheritanceType.JOINED, tableName, schemaName);

            break;

        case TABLE_PER_CLASS:

            // don't override, use original ones.
            model = new InheritanceModel(InheritanceType.TABLE_PER_CLASS, null, null);

            break;

        default:
            // do nothing.
            break;
        }
        return model;
    }

    /**
     * Checks if is inherited.
     * 
     * @return true, if is inherited
     */
    public boolean isInherited()
    {
        return this.model != null;
    }

    /**
     * Gets the inheritence type.
     * 
     * @return the inheritence type
     */
    public InheritanceType getInheritenceType()
    {

        return isInherited() ? this.model.inheritenceType : null;
    }

    /**
     * Gets the discriminator column.
     * 
     * @return the discriminator column
     */
    public String getDiscriminatorColumn()
    {
        return isInherited() ? this.model.discriminatorColumn : null;
    }

    /**
     * Gets the discriminator value.
     * 
     * @return the discriminator value
     */
    public String getDiscriminatorValue()
    {
        return isInherited() ? this.model.discriminatorValue : null;
    }

    /**
     * Gets the table name.
     * 
     * @return the table name
     */
    public String getTableName()
    {
        return isInherited() ? this.model.tableName : null;
    }

    /**
     * Gets the schema name.
     * 
     * @return the schema name
     */
    public String getSchemaName()
    {
        return isInherited() ? this.model.schemaName : null;
    }

    /**
     * The Class InheritanceModel.
     */
    private static class InheritanceModel
    {

        /** The inheritence type. */
        private InheritanceType inheritenceType;

        /** The discriminator column. */
        private String discriminatorColumn;

        /** The discriminator value. */
        private String discriminatorValue;

        /** The table name. */
        private String tableName;

        /** The schema name. */
        private String schemaName;

        /**
         * Instantiates a new inheritance model.
         * 
         * @param type
         *            the type
         * @param discriminatorCol
         *            the discriminator col
         * @param discriminatorValue
         *            the discriminator value
         * @param tableName
         *            the table name
         * @param schemaName
         *            the schema name
         */
        InheritanceModel(final InheritanceType type, final String discriminatorCol, final String discriminatorValue,
                final String tableName, final String schemaName)
        {
            this.inheritenceType = type;
            this.discriminatorColumn = discriminatorCol;
            this.discriminatorValue = discriminatorValue;
            this.tableName = tableName;
            this.schemaName = schemaName;
        }

        /**
         * Instantiates a new inheritance model.
         * 
         * @param type
         *            the type
         * @param tableName
         *            the table name
         * @param schemaName
         *            the schema name
         */
        InheritanceModel(final InheritanceType type, final String tableName, final String schemaName)
        {
            this.inheritenceType = type;
            this.tableName = tableName;
            this.schemaName = schemaName;
        }
    }

    /**
     * Gets the entity annotation.
     * 
     * @return the entity annotation
     */
    public EntityAnnotationProcessor getEntityAnnotation()
    {
        return entityAnnotationProcessor;
    }

    /**
     * Checks for validation constraints.
     * 
     * @return the hasValidationConstraints
     */
    public boolean hasValidationConstraints()
    {
        if (this.hasValidationConstraints)
        {
            return this.hasValidationConstraints;
        }
        if (this.superClazzType != null)
        {
            return ((AbstractManagedType) superClazzType).hasValidationConstraints();
        }
        return false;
    }

    /**
     * Sets the validation constraint present field if an attribute has a
     * constraint present.
     * 
     * @param field
     *            the field
     */
    private void onValidateAttributeConstraints(Field field)
    {

        if (!this.hasValidationConstraints && MetadataUtils.onCheckValidationConstraints(field))
        {

            this.hasValidationConstraints = true;

        }

    }

    /**
     * Sets if a managed type consists of an embeddable attribute.
     * 
     * @param field
     *            the field
     */
    private void onEmbeddableAttribute(Field field)
    {

        if (!this.hasEmbeddableAttribute && MetadataUtils.onCheckEmbeddableAttribute(field))
        {

            this.hasEmbeddableAttribute = true;

        }

    }

    /**
     * Checks for embeddable attribute.
     * 
     * @return the hasEmbeddableAttribute
     */
    public boolean hasEmbeddableAttribute()
    {
        if (this.hasEmbeddableAttribute)
        {
            return this.hasEmbeddableAttribute;
        }
        if (this.superClazzType != null)
        {
            return ((AbstractManagedType) superClazzType).hasEmbeddableAttribute();
        }
        return false;
    }

}
