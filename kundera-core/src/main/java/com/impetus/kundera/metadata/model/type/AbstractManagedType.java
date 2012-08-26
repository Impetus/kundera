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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

/**
 * Implementation for <code>ManagedType</code> interface.
 *
 * @param <X> the generic entity type.
 * @author vivek.mishra
 */
public abstract class AbstractManagedType<X> extends AbstractType<X> implements ManagedType<X>
{
    
    /** The super clazz type. */
    private ManagedType<? super X> superClazzType;

    /** The declared singluar attribs. */
    private Map<String, SingularAttribute<X, ?>> declaredSingluarAttribs;

    /** The declared plural attributes. */
    private Map<String, PluralAttribute<X, ?, ?>> declaredPluralAttributes;

    /**
     * Super constructor with arguments.
     *
     * @param clazz parameterised class.
     * @param persistenceType persistenceType.
     * @param superClazzType the super clazz type
     * @param declaredSingluarAttribs the declared singluar attribs
     * @param declaredPluralAttributes the declared plural attributes
     */
    public AbstractManagedType(Class<X> clazz, javax.persistence.metamodel.Type.PersistenceType persistenceType,
            ManagedType<? super X> superClazzType)
    {
        super(clazz, persistenceType);
        this.superClazzType = superClazzType;
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
            attributes.addAll(superClazzType.getDeclaredAttributes());
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
        SingularAttribute<? super X, Y> attribute = getDeclaredSingularAttribute(paramString, paramClass,false);
        if (superClazzType != null && attribute == null)
        {
            return superClazzType.getDeclaredSingularAttribute(paramString, paramClass);
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
    @SuppressWarnings("unchecked")
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
            Set parentAttrib = superClazzType.getDeclaredSingularAttributes();

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
            pluralAttributes.addAll(superClazzType.getDeclaredPluralAttributes());
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
        Attribute<? super X, ?> attribute = getDeclaredAttribute(paramName,false);
        if (attribute == null && superClazzType != null)
        {
            attribute = superClazzType.getDeclaredAttribute(paramName);
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

    private SingularAttribute<? super X, ?> getSingularAttribute(String paramString, boolean checkValidity)
    {
        SingularAttribute<? super X, ?> attribute = getDeclaredSingularAttribute(paramString, false);

        try
        {
            if (attribute == null && superClazzType != null)
            {
                attribute = superClazzType.getDeclaredSingularAttribute(paramString);
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


    ManagedType<? super X> getSuperClazzType()
    {
        return superClazzType;
    }
    
    public void addSingularAttribute(String attributeName, SingularAttribute<X, ?> attribute)
    {
        if(declaredSingluarAttribs == null)
        {
            declaredSingluarAttribs = new HashMap<String, SingularAttribute<X,?>>();
        }
        
        declaredSingluarAttribs.put(attributeName, attribute);
    }
    
    public void addPluralAttribute(String attributeName, PluralAttribute<X,?,?> attribute)
    {
        if(declaredPluralAttributes== null)
        {
            declaredPluralAttributes = new HashMap<String, PluralAttribute<X,?,?>>();
        }
        
        declaredPluralAttributes.put(attributeName, attribute);
    }
    
    
    /**
     * Gets the declared plural attribute.
     *
     * @param paramName the param name
     * @return the declared plural attribute
     */
    private PluralAttribute<X, ?, ?> getDeclaredPluralAttribute(String paramName)
    {
        return declaredPluralAttributes != null ? declaredPluralAttributes.get(paramName) : null;
    }

    /**
     * Gets the plural attriute.
     *
     * @param paramName the param name
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
     * @param <E> the element type
     * @param pluralAttribute the plural attribute
     * @param paramClass the param class
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
     * @param <E> the element type
     * @param pluralAttribute the plural attribute
     * @param paramClass the param class
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
     * @param <E> the element type
     * @param pluralAttribute the plural attribute
     * @param paramClass the param class
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
     * @param <V> the value type
     * @param pluralAttribute the plural attribute
     * @param valueClazz the value clazz
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
     * @param attribute the attribute
     * @return true, if is collection attribute
     */
    private boolean isCollectionAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.COLLECTION);
    }

    /**
     * Checks if is list attribute.
     *
     * @param attribute the attribute
     * @return true, if is list attribute
     */
    private boolean isListAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.LIST);
    }

    /**
     * Checks if is sets the attribute.
     *
     * @param attribute the attribute
     * @return true, if is sets the attribute
     */
    private boolean isSetAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.SET);
    }

    /**
     * Checks if is map attribute.
     *
     * @param attribute the attribute
     * @return true, if is map attribute
     */
    private boolean isMapAttribute(PluralAttribute<? super X, ?, ?> attribute)
    {
        return attribute != null && attribute.getCollectionType().equals(CollectionType.MAP);
    }

    /**
     * Checks if is bindable.
     *
     * @param <E> the element type
     * @param attribute the attribute
     * @param elementType the element type
     * @return true, if is bindable
     */
    private <E> boolean isBindable(Bindable<?> attribute, Class<E> elementType)
    {
        return attribute != null && attribute.getBindableJavaType().equals(elementType);
    }

    /**
     * Check for valid.
     *
     * @param paramName the param name
     * @param attribute the attribute
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
     * @param paramName
     * @param checkValidity
     * @return
     */
    private Attribute<X, ?> getDeclaredAttribute(String paramName, boolean checkValidity)
    {
        Attribute<X, ?> attribute = (Attribute<X, ?>) getSingularAttribute(paramName,false);

        if (attribute == null)
        {
            attribute = (Attribute<X, ?>) getDeclaredPluralAttribute(paramName);
        }

        if(checkValidity)
        {
            checkForValid(paramName, attribute);
        }
        return attribute;
    }


    /**
     * Returns declared singular attribute.
     * 
     * @param <Y>
     * @param paramString
     * @param paramClass
     * @param checkValidity
     * @return
     */
    private <Y> SingularAttribute<X, Y> getDeclaredSingularAttribute(String paramString, Class<Y> paramClass, boolean checkValidity)
    {
        SingularAttribute<X, ?> declaredAttrib = declaredSingluarAttribs.get(paramString);

        if (declaredAttrib != null && declaredAttrib.getBindableJavaType().equals(paramClass))
        {
            return (SingularAttribute<X, Y>) declaredAttrib;
        }

        if(checkValidity)
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
     * @param checkValidity
     * @return
     */
    private SingularAttribute<X, ?> getDeclaredSingularAttribute(String paramString, boolean checkValidity)
    {
        SingularAttribute<X, ?> attribute = null;
        if (declaredSingluarAttribs != null)
        {
            attribute = declaredSingluarAttribs.get(paramString);
        }

        if(checkValidity)
        {
            checkForValid(paramString, attribute);
        }
        return attribute;
    }


    /**
     * On validity check
     * @param paramString
     * @param checkValidity
     * @param attribute
     */
    private void onValidity(String paramString, boolean checkValidity, SingularAttribute<? super X, ?> attribute)
    {
        if(checkValidity)
        {
            checkForValid(paramString, attribute);
            
        }
    }
}
