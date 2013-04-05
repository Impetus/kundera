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
package com.impetus.kundera.metadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.index.IndexCollection;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Utility class for entity metadata related funcntionality.
 * 
 * @author amresh.singh
 */
public class MetadataUtils
{

    /**
     * Populate column and super column maps.
     * 
     * @param m
     *            the m
     * @param columnNameToFieldMap
     *            the column name to field map
     * @param superColumnNameToFieldMap
     *            the super column name to field map
     */
    public static void populateColumnAndSuperColumnMaps(EntityMetadata m, Map<String, Field> columnNameToFieldMap,
            Map<String, Field> superColumnNameToFieldMap)
    {

        getEmbeddableType(m, columnNameToFieldMap, superColumnNameToFieldMap);
    }

    /**
     * Creates the columns field map.
     * 
     * @param m
     *            the m
     * @param superColumn
     *            the super column
     * @return the map
     */
    public static Map<String, Field> createColumnsFieldMap(EntityMetadata m, EmbeddableType superColumn)
    {
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();

        Set<Attribute> attributes = superColumn.getAttributes();
        for (Attribute column : attributes)
        {
            columnNameToFieldMap.put(((AbstractAttribute) column).getJPAColumnName(), (Field) column.getJavaMember());
        }
        return columnNameToFieldMap;
    }

    /**
     * Creates the super columns field map.
     * 
     * @param m
     *            the m
     * @return the map
     */
    public static Map<String, Field> createSuperColumnsFieldMap(EntityMetadata m)
    {
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        getEmbeddableType(m, null, superColumnNameToFieldMap);
        return superColumnNameToFieldMap;
    }

    /**
     * Gets the embedded collection instance.
     * 
     * @param embeddedCollectionField
     *            the embedded collection field
     * @return the embedded collection instance
     */
    public static Collection getEmbeddedCollectionInstance(Field embeddedCollectionField)
    {
        Collection embeddedCollection = null;
        Class embeddedCollectionFieldClass = embeddedCollectionField.getType();

        if (embeddedCollection == null || embeddedCollection.isEmpty())
        {
            if (embeddedCollectionFieldClass.equals(List.class))
            {
                embeddedCollection = new ArrayList<Object>();
            }
            else if (embeddedCollectionFieldClass.equals(Set.class))
            {
                embeddedCollection = new HashSet<Object>();
            }
            else
            {
                throw new InvalidEntityDefinitionException("Field " + embeddedCollectionField.getName()
                        + " must be either instance of List or Set");
            }
        }
        return embeddedCollection;
    }

    /**
     * Gets the embedded generic object instance.
     * 
     * @param embeddedCollectionField
     *            the embedded collection field
     * @return the embedded generic object instance
     */
    public static Object getEmbeddedGenericObjectInstance(Field embeddedCollectionField)
    {
        Class<?> embeddedClass = PropertyAccessorHelper.getGenericClass(embeddedCollectionField);
        Object embeddedObject = null;
        // must have a default no-argument constructor
        try
        {
            embeddedClass.getConstructor();
            embeddedObject = embeddedClass.newInstance();
        }
        catch (NoSuchMethodException nsme)
        {
            throw new PersistenceException(embeddedClass.getName()
                    + " is @Embeddable and must have a default no-argument constructor.");
        }
        catch (InstantiationException e)
        {
            throw new PersistenceException(embeddedClass.getName() + " could not be instantiated");
        }

        catch (IllegalAccessException e)
        {
            throw new PersistenceException(embeddedClass.getName() + " could not be accessed");
        }
        return embeddedObject;
    }

    /**
     * Gets the embedded collection prefix.
     * 
     * @param embeddedCollectionName
     *            the embedded collection name
     * @return the embedded collection prefix
     */
    public static String getEmbeddedCollectionPrefix(String embeddedCollectionName)
    {
        return embeddedCollectionName.substring(0,
                embeddedCollectionName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER));
    }

    /**
     * Gets the embedded collection postfix.
     * 
     * @param embeddedCollectionName
     *            the embedded collection name
     * @return the embedded collection postfix
     */
    public static String getEmbeddedCollectionPostfix(String embeddedCollectionName)
    {
        return embeddedCollectionName.substring(
                embeddedCollectionName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) + 1,
                embeddedCollectionName.length());
    }

    /**
     * Creates a string representation of a set of foreign keys by combining
     * them together separated by "~" character.
     * 
     * Note: Assumption is that @Id will never contain "~" character. Checks for
     * this are not added yet.
     * 
     * @param foreignKeys
     *            the foreign keys
     * @return the string
     */
    public static String serializeKeys(Set<String> foreignKeys)
    {
        if (null == foreignKeys || foreignKeys.isEmpty())
        {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String key : foreignKeys)
        {
            if (sb.length() > 0)
            {
                sb.append(Constants.FOREIGN_KEY_SEPARATOR);
            }
            sb.append(key);
        }
        return sb.toString();
    }

    /**
     * Splits foreign keys into Set.
     * 
     * @param foreignKeys
     *            the foreign keys
     * @return the set
     */
    public static Set<String> deserializeKeys(String foreignKeys)
    {
        Set<String> keys = new HashSet<String>();

        if (null == foreignKeys || foreignKeys.isEmpty())
        {
            return keys;
        }

        String array[] = foreignKeys.split(Constants.FOREIGN_KEY_SEPARATOR);
        for (String element : array)
        {
            keys.add(element);
        }
        return keys;
    }

    /**
     * Sets the schema and persistence unit.
     * 
     * @param m
     *            the m
     * @param schemaStr
     *            the schema str
     * @param puProperties
     */
    public static void setSchemaAndPersistenceUnit(EntityMetadata m, String schemaStr, Map puProperties)
    {

        if (schemaStr.indexOf(Constants.SCHEMA_PERSISTENCE_UNIT_SEPARATOR) > 0)
        {
            String schemaName = null;
            if (puProperties != null)
            {
                schemaName = (String) puProperties.get(PersistenceProperties.KUNDERA_KEYSPACE);
            }
            if (schemaName == null)
            {
                schemaName = schemaStr.substring(0, schemaStr.indexOf(Constants.SCHEMA_PERSISTENCE_UNIT_SEPARATOR));
            }
            m.setSchema(schemaName);
            m.setPersistenceUnit(schemaStr.substring(
                    schemaStr.indexOf(Constants.SCHEMA_PERSISTENCE_UNIT_SEPARATOR) + 1, schemaStr.length()));
        }
        else
        {
            m.setSchema(schemaStr);
        }
    }

    /**
     * Returns true, if use of secondry index is available, else false.
     * 
     * @param persistenceUnit
     *            persistence unit name
     * @return true, if usage is true in pu. else false.
     */
    public static boolean useSecondryIndex(String persistenceUnit)
    {
        ClientMetadata clientMetadata = KunderaMetadata.INSTANCE.getClientMetadata(persistenceUnit);
        return clientMetadata != null ? clientMetadata.isUseSecondryIndex() : false;
    }

    /**
     * Returns lucene indexing directory.
     * 
     * @param persistenceUnit
     *            persistence unit name
     * @return lucene directory
     */
    public static String getLuceneDirectory(String persistenceUnit)
    {
        if (!useSecondryIndex(persistenceUnit))
        {
            ClientMetadata clientMetadata = KunderaMetadata.INSTANCE.getClientMetadata(persistenceUnit);
            return clientMetadata.getLuceneIndexDir();
        }

        return null;
    }

    public static boolean isParent(EntityMetadata m)
    {
        for (Relation relation : m.getRelations())
        {
            if (Relation.ForeignKey.ONE_TO_MANY.equals(relation.getType()))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns mapped relational name, in case of bi directional mapping, it
     * will return back pKey name of associated entity.
     * 
     * @param relation
     *            holding relation.
     * @return mapped/join column name.
     */
    public static String getMappedName(EntityMetadata parentMetadata, Relation relation)
    {
        if (relation != null)
        {
            String joinColumn = relation.getJoinColumnName();
            if (joinColumn == null)
            {
                Class clazz = relation.getTargetEntity();
                EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(clazz);

                joinColumn = relation.getType().equals(ForeignKey.ONE_TO_MANY) ? ((AbstractAttribute) parentMetadata
                        .getIdAttribute()).getJPAColumnName() : ((AbstractAttribute) metadata.getIdAttribute())
                        .getJPAColumnName();
            }
            return joinColumn;
        }
        return null;
    }

    /**
     * Gets the enclosing document name.
     * 
     * @param m
     *            the m
     * @param criteria
     *            Input criteria
     * @param viaColumnName
     *            true if <code>criteria</code> is column Name, false if
     *            <code>criteria</code> is column field name
     * @return the enclosing document name
     */
    public static String getEnclosingEmbeddedFieldName(EntityMetadata m, String criteria, boolean viaColumnName)
    {
        String enclosingEmbeddedFieldName = null;

        StringTokenizer strToken = new StringTokenizer(criteria, ".");
        String embeddedFieldName = null;
        String embeddableAttributeName = null;

        while (strToken.hasMoreElements())
        {
            embeddableAttributeName = strToken.nextToken();

            if (strToken.countTokens() > 0)
            {
                embeddedFieldName = strToken.nextToken();
            }
        }

        Metamodel metaModel = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());
        EntityType entity = metaModel.entity(m.getEntityClazz());

        try
        {
            Attribute attribute = entity.getAttribute(embeddableAttributeName);

            if (((MetamodelImpl) metaModel).isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
            {
                EmbeddableType embeddable = metaModel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
                Iterator<Attribute> iter = embeddable.getAttributes().iterator();
                while (iter.hasNext())
                {
                    AbstractAttribute attrib = (AbstractAttribute) iter.next();

                    if (viaColumnName && attrib.getName().equals(embeddedFieldName))
                    {
                        enclosingEmbeddedFieldName = attribute.getName();
                        break;
                    }

                    if (!viaColumnName && attrib.getJPAColumnName().equals(embeddedFieldName))
                    {
                        enclosingEmbeddedFieldName = attribute.getName();
                        break;
                    }
                }
            }

        }
        catch (IllegalArgumentException iax)
        {
            return null;
        }
        //
        // if (!m.getColumnFieldNames().contains(criteria))
        // {
        // for (EmbeddedColumn embeddedColumn : m.getEmbeddedColumnsAsList())
        // {
        // List<Column> columns = embeddedColumn.getColumns();
        // for (Column column : columns)
        // {
        // if (viaColumnName && column.getName().equals(criteria))
        // {
        // enclosingEmbeddedFieldName = embeddedColumn.getName();
        // break;
        // }
        //
        // if (!viaColumnName && column.getField().getName().equals(criteria))
        // {
        // enclosingEmbeddedFieldName = embeddedColumn.getName();
        // break;
        // }
        // }
        // }
        //
        // }
        return enclosingEmbeddedFieldName;
    }

    /*
     * public static String getEnclosingEmbeddedFieldName(EntityMetadata m,
     * String columnName) { String enclosingEmbeddedFieldName = null; if
     * (!m.getColumnFieldNames().contains(columnName)) { for (EmbeddedColumn
     * embeddedColumn : m.getEmbeddedColumnsAsList()) { List<Column> columns =
     * embeddedColumn.getColumns(); for (Column column : columns) { if
     * (column.getName().equals(columnName)) { enclosingEmbeddedFieldName =
     * embeddedColumn.getName(); break; } } }
     * 
     * } return enclosingEmbeddedFieldName; }
     */

    private static void getEmbeddableType(EntityMetadata m, Map<String, Field> columnNameToFieldMap,
            Map<String, Field> superColumnNameToFieldMap)
    {
        Metamodel metaModel = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(m.getPersistenceUnit());

        EntityType entityType = metaModel.entity(m.getEntityClazz());

        Set attributes = entityType.getAttributes();
        Iterator<Attribute> iter = attributes.iterator();
        while (iter.hasNext())
        {
            Attribute attribute = iter.next();
            if (((MetamodelImpl) metaModel).isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
            {
                superColumnNameToFieldMap.put(((AbstractAttribute) attribute).getJPAColumnName(),
                        (Field) attribute.getJavaMember());
                if (columnNameToFieldMap != null)
                {
                    getAttributeOfEmbedddable(columnNameToFieldMap, metaModel, attribute);
                }

            }
            else
            {
                columnNameToFieldMap.put(((AbstractAttribute) attribute).getJPAColumnName(),
                        (Field) attribute.getJavaMember());
            }
        }
    }

    private static void getAttributeOfEmbedddable(Map<String, Field> columnNameToFieldMap, Metamodel metaModel,
            Attribute attribute)
    {
        EmbeddableType embeddable = metaModel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());

        Iterator<Attribute> embeddableIter = embeddable.getAttributes().iterator();
        while (embeddableIter.hasNext())
        {
            Attribute embedAttrib = embeddableIter.next();

            // Reason is to avoid in case embeddable attribute within
            // embeddable.
            if (!((MetamodelImpl) metaModel).isEmbeddable(embedAttrib.getJavaType()))
            {
                columnNameToFieldMap.put(((AbstractAttribute) embedAttrib).getJPAColumnName(),
                        (Field) embedAttrib.getJavaMember());
            }
            else
            {
                getAttributeOfEmbedddable(columnNameToFieldMap, metaModel, embedAttrib);
            }
        }
    }

    public static boolean isEmbeddedAtributeIndexable(Field embeddedField)
    {
        Class<?> embeddableClass = PropertyAccessorHelper.getGenericClass(embeddedField);
        Index indexAnn = embeddableClass.getAnnotation(Index.class);
        IndexCollection indexCollection = embeddableClass.getAnnotation(IndexCollection.class);
        if (indexCollection != null && indexCollection.columns() != null)
        {
            return true;
        }
        else if (indexAnn != null)
        {
            if (indexAnn.index())
            {
                return true;
            }
        }
        return false;
    }

    public static boolean isColumnInEmbeddableIndexable(Field embeddedField, String columnFieldName)
    {
        Class<?> embeddableClass = PropertyAccessorHelper.getGenericClass(embeddedField);
        Index indexAnn = embeddableClass.getAnnotation(Index.class);
        IndexCollection indexCollection = embeddableClass.getAnnotation(IndexCollection.class);
        if (indexCollection != null && indexCollection.columns() != null)
        {
            for (com.impetus.kundera.index.Index column : indexCollection.columns())
            {
                if (columnFieldName != null && column != null && column.name() != null
                        && column.name().equals(columnFieldName))
                {
                    return true;
                }
            }
        }
        else if (indexAnn != null && indexAnn.index())
        {
            String[] columnsToBeIndexed = indexAnn.columns();
            if (columnFieldName != null && Arrays.asList(columnsToBeIndexed).contains(columnFieldName))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * If client specific to parameterized persistence unit does not support
     * transaction, return true else will return false.
     * 
     * @param persistenceUnit
     * @return
     */
    public static boolean defaultTransactionSupported(final String persistenceUnit)
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(persistenceUnit);

        String txResource = puMetadata.getProperty(PersistenceProperties.KUNDERA_TRANSACTION_RESOURCE);

        if (txResource == null)
        {
            return true;
        }
        else if (txResource.isEmpty())
        {
            throw new IllegalArgumentException("Property " + PersistenceProperties.KUNDERA_TRANSACTION_RESOURCE
                    + " is blank");
        }
        else
        {
            return false;
        }
    }
}
