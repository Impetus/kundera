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
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.common.CassandraUtilities;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
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
    public static final String CREATE_COLUMNFAMILY_QUERY = "CREATE COLUMNFAMILY $COLUMNFAMILY ($COLUMNS";

    public static final String ADD_PRIMARYKEY_CLAUSE = " , PRIMARY KEY($COLUMNS))";

    public static final String SELECTALL_QUERY = "SELECT * FROM $COLUMNFAMILY";

    public static final String ADD_WHERE_CLAUSE = " WHERE ";

    public static final String SELECT_QUERY = "SELECT $COLUMNS FROM $COLUMNFAMILY";

    public static final String INSERT_QUERY = " INSERT INTO $COLUMNFAMILY($COLUMNS) VALUES($COLUMNVALUES) ";

    public static final String DELETE_QUERY = "DELETE FROM $COLUMNFAMILY";

    public static final String COLUMN_FAMILY = "$COLUMNFAMILY";

    public static final String COLUMNS = "$COLUMNS";

    public static final String COLUMN_VALUES = "$COLUMNVALUES";

    public static final String AND_CLAUSE = " AND ";

    public static final String EQ_CLAUSE = "=";

    public static final String WITH_CLAUSE = " WITH ";

    public static final String QUOTE_STR = "'";

    public static final String LIMIT = " LIMIT ";

    public static final String CREATE_INDEX_QUERY = "CREATE INDEX ON $COLUMNFAMILY ($COLUMNS)";

    public static final String BATCH_QUERY = "BEGIN BATCH $STATEMENT ";

    public static final String STATEMENT = "$STATEMENT";

    public static final String APPLY_BATCH = " APPLY BATCH";

    public static final String USING_CONSISTENCY = "$USING CONSISTENCY";

    public static final String CONSISTENCY_LEVEL = "$CONSISTENCYLEVEL";

    public static final String DROP_TABLE = "drop columnfamily $COLUMN_FAMILY";

    public static final String UPDATE_QUERY = "UPDATE $COLUMNFAMILY ";

    public static final String ADD_SET_CLAUSE = "SET ";

    public static final String COMMA_STR = ", ";

    public static final String INCR_COUNTER = "+";

    public CQLTranslator()
    {

    }

    public static enum TranslationType
    {
        COLUMN, VALUE, ALL;
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
     * @return Map containing translation type as key and string as translated
     *         CQL string.
     */
    public HashMap<TranslationType, String> prepareColumnOrColumnValues(final Object record,
            final EntityMetadata entityMetadata, TranslationType type, Map<String, Object> externalProperties)
    {
        HashMap<TranslationType, String> parsedColumnOrColumnValue = new HashMap<CQLTranslator.TranslationType, String>();
        if (type == null)
        {
            throw new TranslationException("Please specify TranslationType: either COLUMN or VALUE");
        }
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        Class entityClazz = entityMetadata.getEntityClazz();
        EntityType entityType = metaModel.entity(entityClazz);

        StringBuilder builder = new StringBuilder();
        StringBuilder columnBuilder = new StringBuilder();

        onTranslation(record, entityMetadata, type, metaModel, entityClazz, entityType, builder, columnBuilder,
                externalProperties);

        if (type.equals(TranslationType.ALL) || type.equals(TranslationType.VALUE))
        {
            builder.deleteCharAt(builder.length() - 1);
        }

        if (type.equals(TranslationType.ALL) || type.equals(TranslationType.COLUMN))
        {
            columnBuilder.deleteCharAt(columnBuilder.length() - 1);
        }

        parsedColumnOrColumnValue.put(TranslationType.COLUMN, columnBuilder.toString());

        parsedColumnOrColumnValue.put(TranslationType.VALUE, builder.toString());

        return parsedColumnOrColumnValue;
    }

    public static String getCQLType(String internalClazz)
    {
        return InternalToCQLMapper.getType(internalClazz);
    }

    public static String getKeyword(String property)
    {
        return CQLKeywordMapper.getType(property);
    }

    /**
     * On translation to column name or column value based on translation type.
     * 
     * @param record
     *            record
     * @param m
     *            entity metadata
     * @param type
     *            translation type
     * @param metaModel
     *            meta model
     * @param entityClazz
     *            entity class
     * @param entityType
     *            entity type
     * @param builder
     *            column value builder
     * @param columnBuilder
     *            column name builder
     * @param externalProperties
     */
    private void onTranslation(final Object record, final EntityMetadata m, TranslationType type,
            MetamodelImpl metaModel, Class entityClazz, EntityType entityType, StringBuilder builder,
            StringBuilder columnBuilder, Map<String, Object> externalProperties)
    {
        for (Field field : entityClazz.getDeclaredFields())
        {
            if (metaModel.isEmbeddable(field.getType()))
            {
                if (field.getType().equals(m.getIdAttribute().getBindableJavaType()))
                {
                    // builder.
                    // Means it is a compound key! As other
                    // iterate for it's fields to populate it's values in order!
                    EmbeddableType compoundKey = metaModel.embeddable(field.getType());
                    Object compoundKeyObj = PropertyAccessorHelper.getObject(record, field);
                    for (Field compositeColumn : field.getType().getDeclaredFields())
                    {
                        if (!ReflectUtils.isTransientOrStatic(compositeColumn))
                        {
                            onTranslation(type, builder, columnBuilder,
                                    ((AbstractAttribute) (compoundKey.getAttribute(compositeColumn.getName())))
                                            .getJPAColumnName(), compoundKeyObj, compositeColumn);
                        }
                    }
                }
                else
                {
                    throw new PersistenceException(
                            "Super columns are not supported via cql for compound/composite keys!");
                }
            }
            else
            {
                if (!ReflectUtils.isTransientOrStatic(field)
                        && m.getIdAttribute().getName().equals(entityType.getAttribute(field.getName()).getName()))
                {
                    onTranslation(type, builder, columnBuilder,
                            CassandraUtilities.getIdColumnName(m, externalProperties), record, field);
                }
                else if (!ReflectUtils.isTransientOrStatic(field))
                {
                    AbstractAttribute attrib = (AbstractAttribute) entityType.getAttribute(field.getName());

                    if (!attrib.isAssociation())
                    {
                        onTranslation(type, builder, columnBuilder, attrib.getJPAColumnName(), record, field);
                    }
                }
            }
        }
    }

    /**
     * Build where clause with @ EQ_CLAUSE} clause.
     * 
     * @param builder
     * @param field
     * @param member
     * @param entity
     */
    public void buildWhereClause(StringBuilder builder, String field, Field member, Object entity)
    {
        builder = ensureCase(builder, field);
        builder.append(EQ_CLAUSE);
        appendColumnValue(builder, entity, member);
        builder.append(AND_CLAUSE);
    }

    /**
     * Build where clause with given clause.
     * 
     * @param builder
     * @param field
     * @param value
     * @param clause
     */
    public void buildWhereClause(StringBuilder builder, Class fieldClazz, String field, Object value, String clause)
    {
        builder = ensureCase(builder, field);
        builder.append(clause);
        appendValue(builder, fieldClazz, value, false);
        builder.append(AND_CLAUSE);
    }

    /**
     * Builds set clause for a given counter field.
     * 
     * @param builder
     * @param field
     * @param value
     */
    public void buildSetClauseForCounters(StringBuilder builder, String field, Object value)
    {
        builder = ensureCase(builder, field);
        builder.append(EQ_CLAUSE);
        builder = ensureCase(builder, field);
        builder.append(INCR_COUNTER);
        appendValue(builder, value.getClass(), value, false);
        builder.append(COMMA_STR);
    }

    /**
     * Ensures case for corresponding column name.
     * 
     * @param builder
     *            column name builder.
     * @param fieldName
     *            column name.
     * @return builder object with appended column name.
     */
    public StringBuilder ensureCase(StringBuilder builder, String fieldName)
    {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");
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
                builder.append(",");
                appendColumnName(columnBuilder, columnName);
                columnBuilder.append(","); // because only key columns
            }
            break;

        case COLUMN:

            appendColumnName(columnBuilder, columnName);
            columnBuilder.append(","); // because only key columns
            break;

        case VALUE:

            if (appendColumnValue(builder, record, column))
            {
                builder.append(","); // because only key columns
            }
            break;
        }
    }

    /**
     * Appends column value with parametrised builder object. Returns true if
     * value is present.
     * 
     * @param builder
     * @param valueObj
     * @param column
     * @return true if value is not null,else false.
     */
    private boolean appendColumnValue(StringBuilder builder, Object valueObj, Field column)
    {
        Object value = PropertyAccessorHelper.getObject(valueObj, column);
        boolean isPresent = false;
        isPresent = appendValue(builder, column.getType(), value, isPresent);
        return isPresent;
    }

    /**
     * Appends value to builder object for given class type
     * 
     * @param builder
     *            string builder.
     * @param fieldClazz
     *            field class.
     * @param value
     *            value to be appended.
     * @param isPresent
     *            if field is present.
     * @return true, if value is not null else false.
     */
    public boolean appendValue(StringBuilder builder, Class fieldClazz, Object value, boolean isPresent)
    {
        if (value != null)
        {
            isPresent = true;
            // CQL can take string or date within single quotes.

            if (fieldClazz.isAssignableFrom(String.class) || isDate(fieldClazz)
                    || fieldClazz.isAssignableFrom(char.class) || fieldClazz.isAssignableFrom(Character.class)
                    || value instanceof Enum)
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
        return isPresent;
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
        ensureCase(builder, columnName);
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
        ensureCase(builder, columnName);
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

            validationClassMapper.put(IntegerType.class.getSimpleName(), "int");

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

            mapper = Collections.synchronizedMap(validationClassMapper);
        }

        private static final String getType(final String internalClassName)
        {
            return mapper.get(internalClassName);
        }
    }

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

        private static final String getType(final String propertyName)
        {
            return mapper.get(propertyName);
        }
    }

    /**
     * @param builder
     */
    public void buildFilteringClause(StringBuilder builder)
    {
        builder.append(" ALLOW FILTERING");
    }
}
