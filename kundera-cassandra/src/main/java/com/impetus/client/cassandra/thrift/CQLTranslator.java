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
import java.util.Date;
import java.util.HashMap;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorHelper;

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
    public static final String CREATE_COLUMNFAMILY_QUERY = "CREATE COLUMNFAMILY $COLUMNFAMILY ($COLUMNS)";

    public static final String ADD_PRIMARYKEY_CLAUSE = " PRIMARY KEY($COLUMNS)";

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
     * @return Map containing translation type as key and string as translated
     *         CQL string.
     */
    public HashMap<TranslationType, String> prepareColumnOrColumnValues(final Object record,
            final EntityMetadata entityMetadata, TranslationType type)
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
        String columnsAsCSVStr = null;

        StringBuilder builder = new StringBuilder();
        StringBuilder columnBuilder = new StringBuilder();

        onTranslation(record, entityMetadata, type, metaModel, entityClazz, entityType, builder, columnBuilder);

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

    /**
     * On translation to column name or column value based on translation type.
     * 
     * @param record
     *            record
     * @param entityMetadata
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
     */
    private void onTranslation(final Object record, final EntityMetadata entityMetadata, TranslationType type,
            MetamodelImpl metaModel, Class entityClazz, EntityType entityType, StringBuilder builder,
            StringBuilder columnBuilder)
    {
        for (Field field : entityClazz.getDeclaredFields())
        {
            if (metaModel.isEmbeddable(field.getType()))
            {
                if (field.getType().equals(entityMetadata.getIdAttribute().getBindableJavaType()))
                {
                    // builder.
                    // Means it is a compound key! As other
                    // iterate for it's fields to populate it's values in order!
                    EmbeddableType compoundKey = metaModel.embeddable(field.getType());
                    Object compoundKeyObj = PropertyAccessorHelper.getObject(record, field);
                    for (Field compositeColumn : field.getType().getDeclaredFields())
                    {
                        onTranslation(type, builder, columnBuilder,
                                ((AbstractAttribute) (compoundKey.getAttribute(compositeColumn.getName())))
                                        .getJPAColumnName(), compoundKeyObj, compositeColumn);

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
                onTranslation(type, builder, columnBuilder,
                        ((AbstractAttribute) (entityType.getAttribute(field.getName()))).getJPAColumnName(), record,
                        field);

            }

        }
    }

    public void buildWhereClause(StringBuilder builder, String field, Field member, Object entity)
    {
        builder = ensureCase(builder, field);
        builder.append(EQ_CLAUSE);
        appendColumnValue(builder, entity, member);
        builder.append(AND_CLAUSE);
    }
    
    public void buildWhereClause(StringBuilder builder, String field, Object value, String clause)
    {
        builder = ensureCase(builder, field);
        builder.append(clause);
        appendValue(builder, value.getClass(), value, false);
        builder.append(AND_CLAUSE);
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
     * @param compoundKeyObj
     *            value object.
     * @param compositeColumn
     *            value column name.
     */
    private void onTranslation(TranslationType type, StringBuilder builder, StringBuilder columnBuilder,
            String columnName, Object compoundKeyObj, Field compositeColumn)
    {
        switch (type)
        {
        case ALL:
            if (appendColumnValue(builder, compoundKeyObj, compositeColumn))
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

            appendColumnValue(builder, compoundKeyObj, compositeColumn);
            builder.append(","); // because only key columns

            break;
        }
    }

    /**
     * Appends column value with parametrised builder object. Returns true if
     * value is present.
     * 
     * @param builder
     * @param compoundKeyObj
     * @param compositeColumn
     * @return true if value is not null,else false.
     */
    private boolean appendColumnValue(StringBuilder builder, Object compoundKeyObj, Field compositeColumn)
    {
        Object value = PropertyAccessorHelper.getObject(compoundKeyObj, compositeColumn);
        boolean isPresent = false;
        isPresent = appendValue(builder, compositeColumn.getType(), value, isPresent);

        return isPresent;
    }

    /**
     * Appends value to builder object for given class type
     * 
     * @param builder         string builder.
     * @param fieldClazz      field class. 
     * @param value           value to be appended.
     * @param isPresent       if field is present.
     * @return true, if value is not null else false.
     */
    private boolean appendValue(StringBuilder builder, Class fieldClazz, Object value, boolean isPresent)
    {
        if (value != null)
        {
            isPresent = true;
            // CQL can take string or date within single quotes.

            if (fieldClazz.isAssignableFrom(String.class) || isDate(fieldClazz))
            {
                builder.append("'");

                if (isDate(fieldClazz)) // For CQL, date has to
                                                       // be in date.getTime()
                {
                    builder.append(((Date) value).getTime());
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

            // builder.append(","); // because only key columns
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
        // builder.append(","); // because only key columns
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
                || clazz.isAssignableFrom(Timestamp.class) || clazz.isAssignableFrom(Time.class);
    }
}
