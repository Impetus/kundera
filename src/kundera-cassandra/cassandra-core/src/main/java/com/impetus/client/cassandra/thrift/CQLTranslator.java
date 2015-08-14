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
package com.impetus.client.cassandra.thrift;

import java.lang.reflect.Field;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.ElementCollection;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.codec.binary.Hex;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * CQL translator interface, to translate all CRUD operations into CQL queries.
 * In case compound primary key is boolean, we need to
 * $COLUMNS,$COLUMNFAMILY,$COLUMNVALUES : They need to be comma separated.
 * $COLUMNVALUES : It has to be according to data type(add "'" only for
 * text/string)
 * 
 * @author vivek.mishra
 */
public final class CQLTranslator
{

    /** The Constant CREATE_COLUMNFAMILY_QUERY. */
    public static final String CREATE_COLUMNFAMILY_QUERY = "CREATE COLUMNFAMILY $COLUMNFAMILY ($COLUMNS";

    /** The Constant ADD_PRIMARYKEY_CLAUSE. */
    public static final String ADD_PRIMARYKEY_CLAUSE = " , PRIMARY KEY($COLUMNS))";

    /** The Constant SELECTALL_QUERY. */
    public static final String SELECTALL_QUERY = "SELECT * FROM $COLUMNFAMILY";

    /** The Constant SELECT_COUNT_QUERY. */
    public static final String SELECT_COUNT_QUERY = "SELECT COUNT(*) FROM $COLUMNFAMILY";

    /** The Constant ADD_WHERE_CLAUSE. */
    public static final String ADD_WHERE_CLAUSE = " WHERE ";

    /** The Constant SELECT_QUERY. */
    public static final String SELECT_QUERY = "SELECT $COLUMNS FROM $COLUMNFAMILY";

    /** The Constant INSERT_QUERY. */
    public static final String INSERT_QUERY = " INSERT INTO $COLUMNFAMILY($COLUMNS) VALUES($COLUMNVALUES) ";

    /** The Constant DELETE_QUERY. */
    public static final String DELETE_QUERY = "DELETE FROM $COLUMNFAMILY";

    /** The Constant COLUMN_FAMILY. */
    public static final String COLUMN_FAMILY = "$COLUMNFAMILY";

    /** The Constant COLUMNS. */
    public static final String COLUMNS = "$COLUMNS";

    /** The Constant COLUMN_VALUES. */
    public static final String COLUMN_VALUES = "$COLUMNVALUES";

    /** The Constant AND_CLAUSE. */
    public static final String AND_CLAUSE = " AND ";

    /** The Constant SORT_CLAUSE. */
    public static final String SORT_CLAUSE = " ORDER BY ";

    /** The Constant EQ_CLAUSE. */
    public static final String EQ_CLAUSE = "=";

    /** The Constant WITH_CLAUSE. */
    public static final String WITH_CLAUSE = " WITH ";

    /** The Constant QUOTE_STR. */
    public static final String QUOTE_STR = "'";

    /** The Constant LIMIT. */
    public static final String LIMIT = " LIMIT ";

    /** The Constant CREATE_INDEX_QUERY. */
    public static final String CREATE_INDEX_QUERY = "CREATE INDEX ON $COLUMNFAMILY ($COLUMNS)";

    /** The Constant BATCH_QUERY. */
    public static final String BATCH_QUERY = "BEGIN BATCH $STATEMENT ";

    /** The Constant STATEMENT. */
    public static final String STATEMENT = "$STATEMENT";

    /** The Constant APPLY_BATCH. */
    public static final String APPLY_BATCH = " APPLY BATCH";

    /** The Constant USING_CONSISTENCY. */
    public static final String USING_CONSISTENCY = "$USING CONSISTENCY";

    /** The Constant CONSISTENCY_LEVEL. */
    public static final String CONSISTENCY_LEVEL = "$CONSISTENCYLEVEL";

    /** The Constant DROP_TABLE. */
    public static final String DROP_TABLE = "drop columnfamily $COLUMN_FAMILY";

    /** The Constant UPDATE_QUERY. */
    public static final String UPDATE_QUERY = "UPDATE $COLUMNFAMILY ";

    /** The Constant ADD_SET_CLAUSE. */
    public static final String ADD_SET_CLAUSE = "SET ";

    /** The Constant COMMA_STR. */
    public static final String COMMA_STR = ", ";

    /** The Constant INCR_COUNTER. */
    public static final String INCR_COUNTER = "+";

    /** The Constant TOKEN. */
    public static final String TOKEN = "token(";

    /** The Constant CLOSE_BRACKET. */
    public static final String CLOSE_BRACKET = ")";

    /** The Constant SPACE_STRING. */
    public static final String SPACE_STRING = " ";

    /** The Constant IN_CLAUSE. */
    public static final String IN_CLAUSE = "IN";

    /** The Constant OPEN_BRACKET. */
    public static final String OPEN_BRACKET = "(";

    /** The Constant CREATE_COLUMNFAMILY_CLUSTER_ORDER. */
    public static final String CREATE_COLUMNFAMILY_CLUSTER_ORDER = " WITH CLUSTERING ORDER BY ($COLUMNS";

    /** The Constant DEFAULT_KEY_NAME. */
    public static final String DEFAULT_KEY_NAME = "key";

    /** The Constant CREATE_KEYSPACE. */
    public static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH REPLICATION = { 'class':'$CLASS',$REPLICATION} and durable_writes = '$DURABLE_WRITES'";

    /** The Constant SIMPLE_REPLICATION. */
    public static final String SIMPLE_REPLICATION = "'replication_factor':$REPLICATION_FACTOR";

    /** The Constant DURABLE_WRITES. */
    public static final String DURABLE_WRITES = "durable_writes=$DURABLE_WRITES";

    /** The Constant CREATE_TYPE. */
    public static final String CREATE_TYPE = "CREATE TYPE IF NOT EXISTS $TYPE ($COLUMNS";

    /** The Constant TYPE. */
    public static final String TYPE = "$TYPE";

    /** The Constant FROZEN. */
    public static final String FROZEN = "frozen";

    /** The Constant BEGIN_COUNTER_BATCH. */
    public static final String BEGIN_COUNTER_BATCH = "BEGIN COUNTER BATCH";

    /** The Constant BEGIN_BATCH. */
    public static final String BEGIN_BATCH = "BEGIN BATCH";

    /**
     * Instantiates a new CQL translator.
     */
    public CQLTranslator()
    {

    }

    /**
     * The Enum TranslationType.
     */
    public static enum TranslationType
    {

        /** The column. */
        COLUMN,
        /** The value. */
        VALUE,
        /** The all. */
        ALL;
    }

    /**
     * Prepares column name or column values.
     * 
     * @param record
     *            entity.
     * @param entityMetadata
     *            entity meta data
     * @param type
     *            translation type.
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     * @return Map containing translation type as key and string as translated
     *         CQL string.
     */
    public HashMap<TranslationType, Map<String, StringBuilder>> prepareColumnOrColumnValues(final Object record,
            final EntityMetadata entityMetadata, TranslationType type, Map<String, Object> externalProperties,
            final KunderaMetadata kunderaMetadata)
    {
        HashMap<TranslationType, Map<String, StringBuilder>> parsedColumnOrColumnValue = new HashMap<CQLTranslator.TranslationType, Map<String, StringBuilder>>();
        if (type == null)
        {
            throw new TranslationException("Please specify TranslationType: either COLUMN or VALUE");
        }
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        Class entityClazz = entityMetadata.getEntityClazz();
        EntityType entityType = metaModel.entity(entityClazz);

        Map<String, StringBuilder> builders = new HashMap<String, StringBuilder>();
        Map<String, StringBuilder> columnBuilders = new HashMap<String, StringBuilder>();

        onTranslation(record, entityMetadata, type, metaModel, entityClazz, entityType, builders, columnBuilders,
                externalProperties, kunderaMetadata);

        for (String tableName : columnBuilders.keySet())
        {
            StringBuilder builder = builders.get(tableName);
            StringBuilder columnBuilder = columnBuilders.get(tableName);

            if (type.equals(TranslationType.ALL) || type.equals(TranslationType.VALUE))
            {
                builder.deleteCharAt(builder.length() - 1);
            }

            if (type.equals(TranslationType.ALL) || type.equals(TranslationType.COLUMN))
            {
                columnBuilder.deleteCharAt(columnBuilder.length() - 1);
            }
        }
        parsedColumnOrColumnValue.put(TranslationType.COLUMN, columnBuilders);

        parsedColumnOrColumnValue.put(TranslationType.VALUE, builders);

        return parsedColumnOrColumnValue;
    }

    /**
     * Gets the CQL type.
     * 
     * @param internalClazz
     *            the internal clazz
     * @return the CQL type
     */
    public static String getCQLType(String internalClazz)
    {
        return InternalToCQLMapper.getType(internalClazz);
    }

    /**
     * Gets the keyword.
     * 
     * @param property
     *            the property
     * @return the keyword
     */
    public static String getKeyword(String property)
    {
        return CQLKeywordMapper.getType(property);
    }

    /**
     * On translation.
     * 
     * @param record
     *            the record
     * @param m
     *            the m
     * @param type
     *            the type
     * @param metaModel
     *            the meta model
     * @param entityClazz
     *            the entity clazz
     * @param entityType
     *            the entity type
     * @param builders
     *            the builders
     * @param columnBuilders
     *            the column builders
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     */
    private void onTranslation(final Object record, final EntityMetadata m, TranslationType type,
            MetamodelImpl metaModel, Class entityClazz, EntityType entityType, Map<String, StringBuilder> builders,
            Map<String, StringBuilder> columnBuilders, Map<String, Object> externalProperties,
            final KunderaMetadata kunderaMetadata)
    {
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        while (iterator.hasNext())
        {
            Attribute attribute = iterator.next();

            // Populating table name.
            String tableName = ((AbstractAttribute) attribute).getTableName() != null ? ((AbstractAttribute) attribute)
                    .getTableName() : m.getTableName();

            StringBuilder columnBuilder = columnBuilders.get(tableName);
            if (columnBuilder == null)
            {
                columnBuilder = new StringBuilder();
                columnBuilders.put(tableName, columnBuilder);
            }

            StringBuilder builder = builders.get(tableName);
            if (builder == null)
            {
                builder = new StringBuilder();
                builders.put(tableName, builder);
            }
            Field field = (Field) attribute.getJavaMember();
            if (!attribute.equals(m.getIdAttribute())
                    && !((AbstractAttribute) attribute).getJPAColumnName().equals(
                            ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()))
            {
                if (metaModel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                {
                    // create embedded entity persisting format
                    if (field.isAnnotationPresent(ElementCollection.class))
                    {
                        // handle embeddable collection
                        // check list, map, set
                        // build embedded value
                        StringBuilder elementCollectionValue = buildElementCollectionValue(field, record, metaModel,
                                attribute);
                        columnBuilder.append(Constants.ESCAPE_QUOTE);
                        columnBuilder.append(((AbstractAttribute) attribute).getJPAColumnName());
                        columnBuilder.append(Constants.ESCAPE_QUOTE);
                        columnBuilder.append(Constants.COMMA);
                        builder.append(elementCollectionValue);
                        builder.append(Constants.COMMA);
                    }
                    else
                    {
                        EmbeddableType embeddableKey = metaModel.embeddable(field.getType());
                        Object embeddableKeyObj = PropertyAccessorHelper.getObject(record, field);
                        if (embeddableKeyObj != null)
                        {

                            StringBuilder embeddedValueBuilder = new StringBuilder(Constants.OPEN_CURLY_BRACKET);

                            for (Field embeddableColumn : field.getType().getDeclaredFields())
                            {
                                if (!ReflectUtils.isTransientOrStatic(embeddableColumn))
                                {
                                    AbstractAttribute subAttribute = (AbstractAttribute) embeddableKey
                                            .getAttribute(embeddableColumn.getName());
                                    if (metaModel.isEmbeddable(subAttribute.getBindableJavaType()))
                                    {
                                        // construct map; recursive
                                        // send attribute
                                        if (embeddableColumn.isAnnotationPresent(ElementCollection.class))
                                        {
                                            // build element collection value
                                            StringBuilder elementCollectionValue = buildElementCollectionValue(
                                                    embeddableColumn, embeddableKeyObj, metaModel,
                                                    (Attribute) subAttribute);

                                            appendColumnName(embeddedValueBuilder,
                                                    ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn
                                                            .getName()))).getJPAColumnName());
                                            embeddedValueBuilder.append(Constants.COLON);
                                            embeddedValueBuilder.append(elementCollectionValue);
                                        }
                                        else
                                        {
                                            buildEmbeddedValue(embeddableKeyObj, metaModel, embeddedValueBuilder,
                                                    (SingularAttribute) subAttribute);
                                        }
                                    }
                                    else
                                    {
                                        // append key value
                                        appendColumnName(embeddedValueBuilder,
                                                ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn
                                                        .getName()))).getJPAColumnName());
                                        embeddedValueBuilder.append(Constants.COLON);
                                        appendColumnValue(embeddedValueBuilder, embeddableKeyObj, embeddableColumn);
                                    }
                                    embeddedValueBuilder.append(Constants.COMMA);
                                }
                            }
                            // strip last char and append '}'
                            embeddedValueBuilder.deleteCharAt(embeddedValueBuilder.length() - 1);
                            embeddedValueBuilder.append(Constants.CLOSE_CURLY_BRACKET);
                            // add to columnbuilder and builder
                            columnBuilder.append(Constants.ESCAPE_QUOTE);
                            columnBuilder.append(((AbstractAttribute) attribute).getJPAColumnName());
                            columnBuilder.append(Constants.ESCAPE_QUOTE);
                            columnBuilder.append(Constants.COMMA);
                            builder.append(embeddedValueBuilder);
                            builder.append(Constants.COMMA);
                            // end if
                        }
                    }
                }
                else if (!ReflectUtils.isTransientOrStatic(field) && !attribute.isAssociation())
                {
                    onTranslation(type, builder, columnBuilder, ((AbstractAttribute) attribute).getJPAColumnName(),
                            record, field);
                }
            }
        }

        for (String tableName : columnBuilders.keySet())
        {
            translateCompositeId(record, m, type, metaModel, builders, columnBuilders, externalProperties,
                    kunderaMetadata, tableName, m.getIdAttribute());
        }

        // on inherited columns.
        onDiscriminatorColumn(builders.get(m.getTableName()), columnBuilders.get(m.getTableName()), entityType);
    }

    /**
     * Builds the element collection value.
     * 
     * @param field
     *            the field
     * @param record
     *            the record
     * @param metaModel
     *            the meta model
     * @param attribute
     *            the attribute
     * @return the string builder
     */
    private StringBuilder buildElementCollectionValue(Field field, Object record, MetamodelImpl metaModel,
            Attribute attribute)
    {
        StringBuilder elementCollectionValueBuilder = new StringBuilder();
        EmbeddableType embeddableKey = metaModel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
        ((AbstractAttribute) attribute).getJavaMember();
        Object value = PropertyAccessorHelper.getObject(record, field);
        boolean isPresent = false;
        if (Collection.class.isAssignableFrom(field.getType()))
        {

            if (value instanceof Collection)
            {
                Collection collection = ((Collection) value);
                isPresent = true;
                if (List.class.isAssignableFrom(field.getType()))
                {
                    elementCollectionValueBuilder.append(Constants.OPEN_SQUARE_BRACKET);
                }
                if (Set.class.isAssignableFrom(field.getType()))
                {
                    elementCollectionValueBuilder.append(Constants.OPEN_CURLY_BRACKET);
                }
                for (Object o : collection)
                {
                    // Allowing null values.
                    // build embedded value
                    if (o != null)
                    {

                        StringBuilder embeddedValueBuilder = new StringBuilder(Constants.OPEN_CURLY_BRACKET);

                        for (Field embeddableColumn : ((AbstractAttribute) attribute).getBindableJavaType()
                                .getDeclaredFields())
                        {
                            if (!ReflectUtils.isTransientOrStatic(embeddableColumn))
                            {
                                AbstractAttribute subAttribute = (AbstractAttribute) embeddableKey
                                        .getAttribute(embeddableColumn.getName());
                                if (metaModel.isEmbeddable(subAttribute.getBindableJavaType()))
                                {
                                    // construct map; recursive
                                    // send attribute
                                    if (embeddableColumn.getType().isAnnotationPresent(ElementCollection.class))
                                    {
                                        // build element collection value
                                        StringBuilder elementCollectionValue = buildElementCollectionValue(
                                                embeddableColumn, o, metaModel, (Attribute) subAttribute);
                                        appendColumnName(embeddedValueBuilder,
                                                ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn
                                                        .getName()))).getJPAColumnName());
                                        embeddedValueBuilder.append(Constants.COLON);
                                        embeddedValueBuilder.append(elementCollectionValue);
                                    }
                                    else
                                    {
                                        buildEmbeddedValue(o, metaModel, embeddedValueBuilder,
                                                (SingularAttribute) subAttribute);
                                    }
                                }
                                else
                                {
                                    // append key value
                                    appendColumnName(
                                            embeddedValueBuilder,
                                            ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn.getName())))
                                                    .getJPAColumnName());
                                    embeddedValueBuilder.append(Constants.COLON);
                                    appendColumnValue(embeddedValueBuilder, o, embeddableColumn);
                                }
                                embeddedValueBuilder.append(Constants.COMMA);
                            }
                        }
                        // strip last char and append '}'
                        embeddedValueBuilder.deleteCharAt(embeddedValueBuilder.length() - 1);
                        embeddedValueBuilder.append(Constants.CLOSE_CURLY_BRACKET);
                        // add to columnbuilder and builder
                        elementCollectionValueBuilder.append(embeddedValueBuilder);
                        // end if
                    }

                    elementCollectionValueBuilder.append(Constants.COMMA);
                }
                if (!collection.isEmpty())
                {
                    elementCollectionValueBuilder.deleteCharAt(elementCollectionValueBuilder.length() - 1);
                }
                if (List.class.isAssignableFrom(field.getType()))
                {
                    elementCollectionValueBuilder.append(Constants.CLOSE_SQUARE_BRACKET);
                }
                if (Set.class.isAssignableFrom(field.getType()))
                {
                    elementCollectionValueBuilder.append(Constants.CLOSE_CURLY_BRACKET);
                }
                return elementCollectionValueBuilder;
            }
            return null;

        }

        else if (Map.class.isAssignableFrom(field.getType()))
        {
            if (value instanceof Map)
            {
                Map map = ((Map) value);
                isPresent = true;
                elementCollectionValueBuilder.append(Constants.OPEN_CURLY_BRACKET);
                for (Object mapKey : map.keySet())
                {
                    Object mapValue = map.get(mapKey);
                    // Allowing null keys.
                    // key is basic type.. no support for embeddable keys
                    appendValue(elementCollectionValueBuilder, mapKey != null ? mapKey.getClass() : null, mapKey, false);
                    elementCollectionValueBuilder.append(Constants.COLON);
                    // Allowing null values.
                    if (mapValue != null)
                    {

                        StringBuilder embeddedValueBuilder = new StringBuilder(Constants.OPEN_CURLY_BRACKET);

                        for (Field embeddableColumn : ((AbstractAttribute) attribute).getBindableJavaType()
                                .getDeclaredFields())
                        {
                            if (!ReflectUtils.isTransientOrStatic(embeddableColumn))
                            {
                                AbstractAttribute subAttribute = (AbstractAttribute) embeddableKey
                                        .getAttribute(embeddableColumn.getName());
                                if (metaModel.isEmbeddable(subAttribute.getBindableJavaType()))
                                {
                                    // construct map; recursive
                                    // send attribute
                                    if (embeddableColumn.getType().isAnnotationPresent(ElementCollection.class))
                                    {
                                        // build element collection value
                                        StringBuilder elementCollectionValue = buildElementCollectionValue(
                                                embeddableColumn, mapValue, metaModel, (Attribute) subAttribute);
                                        appendColumnName(embeddedValueBuilder,
                                                ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn
                                                        .getName()))).getJPAColumnName());
                                        embeddedValueBuilder.append(Constants.COLON);
                                        embeddedValueBuilder.append(elementCollectionValue);
                                    }
                                    else
                                    {
                                        buildEmbeddedValue(mapValue, metaModel, embeddedValueBuilder,
                                                (SingularAttribute) subAttribute);
                                    }
                                }
                                else
                                {
                                    // append key value
                                    appendColumnName(
                                            embeddedValueBuilder,
                                            ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn.getName())))
                                                    .getJPAColumnName());
                                    embeddedValueBuilder.append(Constants.COLON);
                                    appendColumnValue(embeddedValueBuilder, mapValue, embeddableColumn);

                                }
                                embeddedValueBuilder.append(Constants.COMMA);
                            }

                        }
                        // strip last char and append '}'
                        embeddedValueBuilder.deleteCharAt(embeddedValueBuilder.length() - 1);
                        embeddedValueBuilder.append(Constants.CLOSE_CURLY_BRACKET);
                        // add to columnbuilder and builder
                        elementCollectionValueBuilder.append(embeddedValueBuilder);
                        // end if
                    }
                    elementCollectionValueBuilder.append(Constants.COMMA);
                }
                if (!map.isEmpty())
                {
                    elementCollectionValueBuilder.deleteCharAt(elementCollectionValueBuilder.length() - 1);
                }

                elementCollectionValueBuilder.append(Constants.CLOSE_CURLY_BRACKET);
                return elementCollectionValueBuilder;
            }
            return null;
        }
        return null;
    }

    /**
     * Builds the embedded value.
     * 
     * @param record
     *            the record
     * @param metaModel
     *            the meta model
     * @param embeddedValueBuilder
     *            the embedded value builder
     * @param attribute
     *            the attribute
     */
    private void buildEmbeddedValue(final Object record, MetamodelImpl metaModel, StringBuilder embeddedValueBuilder,
            SingularAttribute attribute)
    {
        // TODO Auto-generated method stub
        Field field = (Field) attribute.getJavaMember();

        EmbeddableType embeddableKey = metaModel.embeddable(field.getType());
        Object embeddableKeyObj = PropertyAccessorHelper.getObject(record, field);
        if (embeddableKeyObj != null)
        {
            StringBuilder tempBuilder = new StringBuilder();
            tempBuilder.append(Constants.OPEN_CURLY_BRACKET);

            for (Field embeddableColumn : field.getType().getDeclaredFields())
            {
                if (!ReflectUtils.isTransientOrStatic(embeddableColumn))
                {
                    Attribute subAttribute = (SingularAttribute) embeddableKey.getAttribute(embeddableColumn.getName());
                    if (metaModel.isEmbeddable(((AbstractAttribute) subAttribute).getBindableJavaType()))
                    {
                        // construct map; recursive
                        // send attribute
                        buildEmbeddedValue(embeddableKeyObj, metaModel, tempBuilder, (SingularAttribute) subAttribute);
                    }
                    else
                    {
                        // append key value
                        appendColumnName(tempBuilder, ((AbstractAttribute) (embeddableKey.getAttribute(embeddableColumn
                                .getName()))).getJPAColumnName());
                        tempBuilder.append(Constants.COLON);
                        appendColumnValue(tempBuilder, embeddableKeyObj, embeddableColumn);
                    }
                    tempBuilder.append(Constants.COMMA);
                }

            }
            // strip last char and append '}'
            tempBuilder.deleteCharAt(tempBuilder.length() - 1);
            tempBuilder.append(Constants.CLOSE_CURLY_BRACKET);
            appendColumnName(embeddedValueBuilder, ((AbstractAttribute) attribute).getJPAColumnName());
            embeddedValueBuilder.append(Constants.COLON);
            embeddedValueBuilder.append(tempBuilder);
        }
        else
        {
            embeddedValueBuilder.deleteCharAt(embeddedValueBuilder.length() - 1);
        }
    }

    /**
     * Translate composite id.
     * 
     * @param record
     *            the record
     * @param m
     *            the m
     * @param type
     *            the type
     * @param metaModel
     *            the meta model
     * @param builders
     *            the builders
     * @param columnBuilders
     *            the column builders
     * @param externalProperties
     *            the external properties
     * @param kunderaMetadata
     *            the kundera metadata
     * @param tableName
     *            the table name
     * @param attribute
     *            the attribute
     */
    private void translateCompositeId(final Object record, final EntityMetadata m, TranslationType type,
            MetamodelImpl metaModel, Map<String, StringBuilder> builders, Map<String, StringBuilder> columnBuilders,
            Map<String, Object> externalProperties, final KunderaMetadata kunderaMetadata, String tableName,
            SingularAttribute attribute)
    {
        StringBuilder builder = builders.get(tableName);
        StringBuilder columnBuilder = columnBuilders.get(tableName);
        Field field = (Field) attribute.getJavaMember();
        if (metaModel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
        {
            // builder.
            // Means it is a compound key! As other
            // iterate for it's fields to populate it's values in
            // order!
            EmbeddableType compoundKey = metaModel.embeddable(field.getType());
            Object compoundKeyObj = PropertyAccessorHelper.getObject(record, field);
            for (Field compositeColumn : field.getType().getDeclaredFields())
            {
                if (!ReflectUtils.isTransientOrStatic(compositeColumn))
                {
                    attribute = (SingularAttribute) compoundKey.getAttribute(compositeColumn.getName());
                    if (metaModel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType()))
                    {
                        translateCompositeId(compoundKeyObj, m, type, metaModel, builders, columnBuilders,
                                externalProperties, kunderaMetadata, tableName, attribute);
                    }
                    else
                    {
                        onTranslation(type, builder, columnBuilder,
                                ((AbstractAttribute) (compoundKey.getAttribute(compositeColumn.getName())))
                                        .getJPAColumnName(), compoundKeyObj, compositeColumn);
                    }
                }
            }
        }
        else if (!ReflectUtils.isTransientOrStatic(field))
        {
            onTranslation(type, builder, columnBuilder,
                    CassandraUtilities.getIdColumnName(kunderaMetadata, m, externalProperties, true), record, field);
        }
    }

    /**
     * On discriminator column.
     * 
     * @param builder
     *            the builder
     * @param columnBuilder
     *            the column builder
     * @param entityType
     *            the entity type
     */
    private void onDiscriminatorColumn(StringBuilder builder, StringBuilder columnBuilder, EntityType entityType)
    {
        String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        // No need to check for empty or blank, as considering it as valid name
        // for nosql!
        if (discrColumn != null && discrValue != null)
        {
            appendValue(builder, String.class, discrValue, false);
            builder.append(Constants.COMMA);
            appendColumnName(columnBuilder, discrColumn);
            columnBuilder.append(Constants.COMMA); // because only key columns

        }
    }

    /**
     * Build where clause with @ EQ_CLAUSE} clause.
     * 
     * @param builder
     *            the builder
     * @param field
     *            the field
     * @param member
     *            the member
     * @param entity
     *            the entity
     */
    public void buildWhereClause(StringBuilder builder, String field, Field member, Object entity)
    {
        // builder = ensureCase(builder, field, false);
        // builder.append(EQ_CLAUSE);
        // appendColumnValue(builder, entity, member);
        // builder.append(AND_CLAUSE);
        Object value = PropertyAccessorHelper.getObject(entity, member);
        buildWhereClause(builder, member.getType(), field, value, EQ_CLAUSE, false);
    }

    /**
     * Build where clause with given clause.
     * 
     * @param builder
     *            the builder
     * @param fieldClazz
     *            the field clazz
     * @param field
     *            the field
     * @param value
     *            the value
     * @param clause
     *            the clause
     * @param useToken
     *            the use token
     */
    public void buildWhereClause(StringBuilder builder, Class fieldClazz, String field, Object value, String clause,
            boolean useToken)
    {

        builder = onWhereClause(builder, fieldClazz, field, value, clause, useToken);
        builder.append(AND_CLAUSE);
    }

    /**
     * Build where clause with given clause.
     * 
     * @param builder
     *            the builder
     * @param fieldClazz
     *            the field clazz
     * @param field
     *            the field
     * @param value
     *            the value
     * @param clause
     *            the clause
     * @param useToken
     *            the use token
     * @return the string builder
     */
    public StringBuilder onWhereClause(StringBuilder builder, Class fieldClazz, String field, Object value,
            String clause, boolean useToken)
    {

        if (clause.trim().equals(IN_CLAUSE))
        {
            useToken = false;
        }

        builder = ensureCase(builder, field, useToken);
        builder.append(SPACE_STRING);
        if (fieldClazz.isAssignableFrom(List.class) || fieldClazz.isAssignableFrom(Map.class)
                || fieldClazz.isAssignableFrom(Set.class))
        {
            builder.append("CONTAINS");
        }
        else
        {
            builder.append(clause);
        }
        builder.append(SPACE_STRING);

        if (clause.trim().equals(IN_CLAUSE))
        {
            builder.append(OPEN_BRACKET);
            String itemValues = String.valueOf(value);
            itemValues = itemValues.startsWith(OPEN_BRACKET) && itemValues.endsWith(CLOSE_BRACKET) ? itemValues
                    .substring(1, itemValues.length() - 1) : itemValues;
            List<String> items = Arrays.asList(((String) itemValues).split("\\s*,\\s*"));
            int counter = 0;
            for (String str : items)
            {
                str = str.trim();
                str = (str.startsWith(Constants.ESCAPE_QUOTE) && str.endsWith(Constants.ESCAPE_QUOTE))
                        || (str.startsWith("'") && str.endsWith("'")) ? str.substring(1, str.length() - 1) : str;
                appendValue(builder, fieldClazz, str, false, false);
                counter++;
                if (counter < items.size())
                {
                    builder.append(COMMA_STR);
                }

            }
            builder.append(CLOSE_BRACKET);
        }
        else
        {
            appendValue(builder, fieldClazz, value, false, useToken);
        }
        return builder;
    }

    /**
     * Builds set clause for a given counter field.
     * 
     * @param builder
     *            the builder
     * @param field
     *            the field
     * @param value
     *            the value
     */
    public void buildSetClauseForCounters(StringBuilder builder, String field, Object value)
    {
        builder = ensureCase(builder, field, false);
        builder.append(EQ_CLAUSE);
        builder = ensureCase(builder, field, false);
        builder.append(INCR_COUNTER);
        appendValue(builder, value.getClass(), value, false, false);
        builder.append(COMMA_STR);
    }

    /**
     * Builds set clause for a given field.
     * 
     * @param m
     *            the m
     * @param builder
     *            the builder
     * @param property
     *            the property
     * @param value
     *            the value
     */
    public void buildSetClause(EntityMetadata m, StringBuilder builder, String property, Object value)
    {
        builder = ensureCase(builder, property, false);
        builder.append(EQ_CLAUSE);

        if (m.isCounterColumnType())
        {
            builder = ensureCase(builder, property, false);
            builder.append(INCR_COUNTER);
            builder.append(value);
        }
        else
        {
            appendValue(builder, value.getClass(), value, false, false);
        }

        builder.append(COMMA_STR);
    }

    /**
     * Ensures case for corresponding column name.
     * 
     * @param builder
     *            column name builder.
     * @param fieldName
     *            column name.
     * @param useToken
     *            the use token
     * @return builder object with appended column name.
     */
    public StringBuilder ensureCase(StringBuilder builder, String fieldName, boolean useToken)
    {
        if (useToken)
        {
            builder.append(TOKEN);
        }
        builder.append(Constants.ESCAPE_QUOTE);
        builder.append(fieldName);
        builder.append(Constants.ESCAPE_QUOTE);
        if (useToken)
        {
            builder.append(CLOSE_BRACKET);
        }
        return builder;

    }

    /**
     * Translates input object and corresponding field based on: a) ALL :
     * translate both column name and column value. b) COlUMN: translates column
     * name only. c) VALUE: translates column value only.
     * 
     * @param type
     *            translation type.
     * @param builder
     *            column value builder object.
     * @param columnBuilder
     *            column name builder object.
     * @param columnName
     *            column name.
     * @param record
     *            value object.
     * @param column
     *            value column name.
     */
    private void onTranslation(TranslationType type, StringBuilder builder, StringBuilder columnBuilder,
            String columnName, Object record, Field column)
    {
        switch (type)
        {
        case ALL:
            if (appendColumnValue(builder, record, column))
            {
                builder.append(Constants.COMMA);
                appendColumnName(columnBuilder, columnName);
                columnBuilder.append(Constants.COMMA); // because only key columns
            }
            break;

        case COLUMN:

            appendColumnName(columnBuilder, columnName);
            columnBuilder.append(Constants.COMMA); // because only key columns
            break;

        case VALUE:

            if (appendColumnValue(builder, record, column))
            {
                builder.append(Constants.COMMA); // because only key columns
            }
            break;
        }
    }

    /**
     * Appends column value with parametrised builder object. Returns true if
     * value is present.
     * 
     * @param builder
     *            the builder
     * @param valueObj
     *            the value obj
     * @param column
     *            the column
     * @return true if value is not null,else false.
     */
    private boolean appendColumnValue(StringBuilder builder, Object valueObj, Field column)
    {
        Object value = PropertyAccessorHelper.getObject(valueObj, column);
        boolean isPresent = false;
        isPresent = appendValue(builder, column.getType(), value, isPresent, false);
        return isPresent;
    }

    /**
     * Appends value to builder object for given class type.
     * 
     * @param builder
     *            string builder.
     * @param fieldClazz
     *            field class.
     * @param value
     *            value to be appended.
     * @param isPresent
     *            if field is present.
     * @param useToken
     *            the use token
     * @return true, if value is not null else false.
     */
    public boolean appendValue(StringBuilder builder, Class fieldClazz, Object value, boolean isPresent,
            boolean useToken)
    {
        if (List.class.isAssignableFrom(fieldClazz))
        {
            isPresent = appendList(builder, value != null ? value : new ArrayList());
        }

        else if (Set.class.isAssignableFrom(fieldClazz))
        {
            isPresent = appendSet(builder, value != null ? value : new HashSet());
        }

        else if (Map.class.isAssignableFrom(fieldClazz))
        {
            isPresent = appendMap(builder, value != null ? value : new HashMap());
        }
        else
        {
            isPresent = true;
            appendValue(builder, fieldClazz, value, useToken);
        }
        return isPresent;
    }

    /**
     * Appends a object of type {@link java.util.List}
     * 
     * @param builder
     *            the builder
     * @param value
     *            the value
     * @return true, if successful
     */
    private boolean appendList(StringBuilder builder, Object value)
    {
        boolean isPresent = false;
        if (value instanceof Collection)
        {
            Collection collection = ((Collection) value);
            isPresent = true;
            builder.append(Constants.OPEN_SQUARE_BRACKET);
            for (Object o : collection)
            {
                // Allowing null values.
                appendValue(builder, o != null ? o.getClass() : null, o, false);
                builder.append(Constants.COMMA);
            }
            if (!collection.isEmpty())
            {
                builder.deleteCharAt(builder.length() - 1);
            }
            builder.append(Constants.CLOSE_SQUARE_BRACKET);

        }
        else
        {
            appendValue(builder, value.getClass(), value, false);
        }
        return isPresent;
    }

    /**
     * Appends a object of type {@link java.util.Map}
     * 
     * @param builder
     *            the builder
     * @param value
     *            the value
     * @return true, if successful
     */
    private boolean appendSet(StringBuilder builder, Object value)
    {
        boolean isPresent = false;
        if (value instanceof Collection)
        {
            Collection collection = ((Collection) value);
            isPresent = true;
            builder.append(Constants.OPEN_CURLY_BRACKET);
            for (Object o : collection)
            {
                // Allowing null values.
                appendValue(builder, o != null ? o.getClass() : null, o, false);
                builder.append(Constants.COMMA);
            }
            if (!collection.isEmpty())
            {
                builder.deleteCharAt(builder.length() - 1);
            }
            builder.append(Constants.CLOSE_CURLY_BRACKET);
        }
        else
        {
            appendValue(builder, value.getClass(), value, false);
        }
        return isPresent;
    }

    /**
     * Appends a object of type {@link java.util.List}
     * 
     * @param builder
     *            the builder
     * @param value
     *            the value
     * @return true, if successful
     */
    private boolean appendMap(StringBuilder builder, Object value)
    {
        boolean isPresent = false;
        if (value instanceof Map)
        {
            Map map = ((Map) value);
            isPresent = true;
            builder.append(Constants.OPEN_CURLY_BRACKET);
            for (Object mapKey : map.keySet())
            {
                Object mapValue = map.get(mapKey);
                // Allowing null keys.
                appendValue(builder, mapKey != null ? mapKey.getClass() : null, mapKey, false);
                builder.append(Constants.COLON);
                // Allowing null values.
                appendValue(builder, mapValue != null ? mapValue.getClass() : null, mapValue, false);
                builder.append(Constants.COMMA);
            }
            if (!map.isEmpty())
            {
                builder.deleteCharAt(builder.length() - 1);
            }

            builder.append(Constants.CLOSE_CURLY_BRACKET);
        }
        else
        {
            appendValue(builder, value.getClass(), value, false);
        }
        return isPresent;
    }

    /**
     * Append value.
     * 
     * @param builder
     *            the builder
     * @param fieldClazz
     *            the field clazz
     * @param value
     *            the value
     * @param useToken
     *            the use token
     */
    private void appendValue(StringBuilder builder, Class fieldClazz, Object value, boolean useToken)
    {
        // To allow handle byte array class object by converting it to string

        if (fieldClazz != null && fieldClazz.isAssignableFrom(byte[].class))
        {
            value = value != null ? value : ByteBufferUtil.EMPTY_BYTE_BUFFER.array();
            StringBuilder hexstr = new StringBuilder("0x");
            builder.append(hexstr.append((Hex.encodeHex((byte[]) value))));
        }
        else
        {
            if (useToken)
            {
                builder.append(TOKEN);
            }

            if (fieldClazz != null
                    && value != null
                    && (fieldClazz.isAssignableFrom(String.class) || isDate(fieldClazz)
                            || fieldClazz.isAssignableFrom(char.class) || fieldClazz.isAssignableFrom(Character.class) || value instanceof Enum))
            {

                if (fieldClazz.isAssignableFrom(String.class))
                {
                    // To allow escape character
                    value = ((String) value).replaceAll("'", "''");
                }
                builder.append("'");

                if (isDate(fieldClazz)) // For CQL, date has to
                                        // be in date.getTime()
                {
                    builder.append(PropertyAccessorFactory.getPropertyAccessor(fieldClazz).toString(value));
                }
                else if (value instanceof Enum)
                {
                    builder.append(((Enum) value).name());
                }

                else
                {
                    builder.append(value);
                }
                builder.append("'");
            }
            else
            {
                builder.append(value);
            }

        }
        if (useToken)
        {
            builder.append(CLOSE_BRACKET);
        }
    }

    /**
     * Appends column name and ensure case sensitivity.
     * 
     * @param builder
     *            string builder.
     * @param columnName
     *            column name.
     */
    public void appendColumnName(StringBuilder builder, String columnName)
    {
        ensureCase(builder, columnName, false);
    }

    /**
     * Appends column name and data type also ensures case sensitivity.
     * 
     * @param builder
     *            string builder
     * @param columnName
     *            column name
     * @param dataType
     *            data type.
     */
    public void appendColumnName(StringBuilder builder, String columnName, String dataType)
    {
        ensureCase(builder, columnName, false);
        builder.append(" "); // because only key columns
        builder.append(dataType);
    }

    /**
     * Validates if input class is of type input.
     * 
     * @param clazz
     *            class
     * 
     * @return true, if it is a date field class.
     */
    private boolean isDate(Class clazz)
    {
        return clazz.isAssignableFrom(Date.class) || clazz.isAssignableFrom(java.sql.Date.class)
                || clazz.isAssignableFrom(Timestamp.class) || clazz.isAssignableFrom(Time.class)
                || clazz.isAssignableFrom(Calendar.class);
    }

    /**
     * Maps internal data type of cassandra to CQL type representation.
     * 
     * @author vivek.mishra
     */
    private static class InternalToCQLMapper
    {
        /** The Mapper. */
        private final static Map<String, String> mapper;

        static
        {
            Map<String, String> validationClassMapper = new HashMap<String, String>();
            // TODO: support for ascii is missing!

            // putting possible combination into map.

            validationClassMapper.put(UTF8Type.class.getSimpleName(), "text");

            validationClassMapper.put(IntegerType.class.getSimpleName(), "varint");

            validationClassMapper.put(Int32Type.class.getSimpleName(), "int");

            validationClassMapper.put(DoubleType.class.getSimpleName(), "double");

            validationClassMapper.put(BooleanType.class.getSimpleName(), "boolean");

            validationClassMapper.put(LongType.class.getSimpleName(), "bigint");

            validationClassMapper.put(BytesType.class.getSimpleName(), "blob");

            validationClassMapper.put(FloatType.class.getSimpleName(), "float");

            // missing
            validationClassMapper.put(CounterColumnType.class.getSimpleName(), "counter");

            validationClassMapper.put(DecimalType.class.getSimpleName(), "decimal");

            validationClassMapper.put(UUIDType.class.getSimpleName(), "uuid");

            validationClassMapper.put(DateType.class.getSimpleName(), "timestamp");
            validationClassMapper.put(TimestampType.class.getSimpleName(), "timestamp");

            // collection types
            validationClassMapper.put(ListType.class.getSimpleName(), "list");
            validationClassMapper.put(SetType.class.getSimpleName(), "set");
            validationClassMapper.put(MapType.class.getSimpleName(), "map");

            mapper = Collections.synchronizedMap(validationClassMapper);
        }

        /**
         * Gets the type.
         * 
         * @param internalClassName
         *            the internal class name
         * @return the type
         */
        private static final String getType(final String internalClassName)
        {
            return mapper.get(internalClassName);
        }
    }

    /**
     * The Class CQLKeywordMapper.
     */
    private static class CQLKeywordMapper
    {
        /** The Mapper. */
        private final static Map<String, String> mapper = new HashMap<String, String>();

        // missing: compaction_strategy_options,
        // compression_parameters,sstable_size_in_mb
        static
        {
            mapper.put(CassandraConstants.READ_REPAIR_CHANCE, "read_repair_chance");
            mapper.put(CassandraConstants.DCLOCAL_READ_REPAIR_CHANCE, "dclocal_read_repair_chance");
            mapper.put(CassandraConstants.BLOOM_FILTER_FP_CHANCE, "bloom_filter_fp_chance");
            mapper.put(CassandraConstants.COMPACTION_STRATEGY, "compaction_strategy_class");
            mapper.put(CassandraConstants.BLOOM_FILTER_FP_CHANCE, "bloom_filter_fp_chance");

            // mapper.put(CassandraConstants.COMPARATOR_TYPE, "comparator");

            mapper.put(CassandraConstants.REPLICATE_ON_WRITE, "replicate_on_write");
            mapper.put(CassandraConstants.CACHING, "caching");
            // TODO: these are not supported.
            // mapper.put(CassandraConstants.MAX_COMPACTION_THRESHOLD,
            // "max_compaction_threshold");
            // mapper.put(CassandraConstants.MIN_COMPACTION_THRESHOLD,
            // "min_compaction_threshold");
            mapper.put(CassandraConstants.COMMENT, "comment");
            mapper.put(CassandraConstants.GC_GRACE_SECONDS, "gc_grace_seconds");
        }

        /**
         * Gets the type.
         * 
         * @param propertyName
         *            the property name
         * @return the type
         */
        private static final String getType(final String propertyName)
        {
            return mapper.get(propertyName);
        }
    }

    /**
     * Builds the filtering clause.
     * 
     * @param builder
     *            the builder
     */
    public void buildFilteringClause(StringBuilder builder)
    {
        builder.append(" ALLOW FILTERING");
    }

    /**
     * Builds the order by clause.
     * 
     * @param builder
     *            the builder
     * @param field
     *            the field
     * @param orderType
     *            the order type
     * @param useToken
     *            the use token
     */
    public void buildOrderByClause(StringBuilder builder, String field, Object orderType, boolean useToken)
    {
        builder.append(SPACE_STRING);
        builder.append(SORT_CLAUSE);
        builder = ensureCase(builder, field, useToken);
        builder.append(SPACE_STRING);
        builder.append(orderType);
    }

    /**
     * Builds the select query.
     * 
     * @param descriptor
     *            the descriptor
     * @return the string builder
     */
    public StringBuilder buildSelectQuery(TableGeneratorDiscriptor descriptor)
    {
        StringBuilder builder = new StringBuilder("Select ");
        ensureCase(builder, descriptor.getValueColumnName(), false).append(" from ");
        ensureCase(builder, descriptor.getTable(), false).append(" where ");
        ensureCase(builder, descriptor.getPkColumnName(), false).append(" = '").append(
                descriptor.getPkColumnValue() + "'");

        return builder;
    }

    /**
     * Builds the update query.
     * 
     * @param descriptor
     *            the descriptor
     * @return the string builder
     */
    public StringBuilder buildUpdateQuery(TableGeneratorDiscriptor descriptor)
    {
        StringBuilder builder = new StringBuilder("Update ");
        ensureCase(builder, descriptor.getTable(), false).append(" set ");
        ensureCase(builder, descriptor.getValueColumnName(), false).append(" = ");
        ensureCase(builder, descriptor.getValueColumnName(), false).append(" + ").append(1).append(" where ");
        ensureCase(builder, descriptor.getPkColumnName(), false).append(" = '").append(
                descriptor.getPkColumnValue() + "'");

        return builder;
    }
}