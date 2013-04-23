/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.cassandra.common;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.scale7.cassandra.pelops.Bytes;

import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * Provides utilities methods
 * 
 * @author amresh.singh
 */
public class CassandraUtilities
{

    public static String toUTF8(byte[] value)
    {
        return value == null ? null : new String(value, Charset.forName(Constants.CHARSET_UTF8));
    }

    public static String getKeyspace(String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadata.getProperties();
        String keyspace = (String) props.get(PersistenceProperties.KUNDERA_KEYSPACE);
        return keyspace;
    }

    public static Bytes toBytes(Object value, Field f)
    {
        return toBytes(value, f.getType());

    }

    /**
     * @param value
     * @param f
     * @return
     */
    public static Bytes toBytes(Object value, Class<?> clazz)
    {
        if (clazz.isAssignableFrom(String.class))
        {
            return Bytes.fromByteArray(((String) value).getBytes());
        }
        else if (clazz.equals(int.class) || clazz.isAssignableFrom(Integer.class))
        {
            return Bytes.fromInt(Integer.parseInt(value.toString()));
        }
        else if (clazz.equals(long.class) || clazz.isAssignableFrom(Long.class))
        {
            return Bytes.fromLong(Long.parseLong(value.toString()));
        }
        else if (clazz.equals(boolean.class) || clazz.isAssignableFrom(Boolean.class))
        {
            return Bytes.fromBoolean(Boolean.valueOf(value.toString()));
        }
        else if (clazz.equals(double.class) || clazz.isAssignableFrom(Double.class))
        {
            return Bytes.fromDouble(Double.valueOf(value.toString()));
        }
        else if (clazz.isAssignableFrom(java.util.UUID.class))
        {
            return Bytes.fromUuid(UUID.fromString(value.toString()));
        }
        else if (clazz.equals(float.class) || clazz.isAssignableFrom(Float.class))
        {
            return Bytes.fromFloat(Float.valueOf(value.toString()));
        }
        else if (clazz.isAssignableFrom(Date.class))
        {
            DateAccessor dateAccessor = new DateAccessor();
            return Bytes.fromByteArray(dateAccessor.toBytes(value));
        }
        else
        {
            if (value.getClass().isAssignableFrom(String.class))
            {
                value = PropertyAccessorFactory.getPropertyAccessor(clazz).fromString(clazz, value.toString());
            }
            return Bytes.fromByteArray(PropertyAccessorFactory.getPropertyAccessor(clazz).toBytes(value));
        }
    }

    /**
     * Append columns.
     * 
     * @param builder
     *            the builder
     * @param columns
     *            the columns
     * @param selectQuery
     *            the select query
     * @param translator
     *            the translator
     */
    public static StringBuilder appendColumns(StringBuilder builder, List<String> columns, String selectQuery,
            CQLTranslator translator)
    {
        if (columns != null)
        {
            for (String column : columns)
            {
                translator.appendColumnName(builder, column);
                builder.append(",");
            }
        }
        if (builder.lastIndexOf(",") != -1)
        {
            builder.deleteCharAt(builder.length() - 1);
            // selectQuery = StringUtils.replace(selectQuery,
            // CQLTranslator.COLUMN_FAMILY, builder.toString());
            selectQuery = StringUtils.replace(selectQuery, CQLTranslator.COLUMNS, builder.toString());
        }

        builder = new StringBuilder(selectQuery);
        return builder;
    }

    public static String getIdColumnName(EntityMetadata m, Map<String, Object> externalProperties)
    {
        String persistenceUnit = m.getPersistenceUnit();
        PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String autoDdlOption = externalProperties != null ? (String) externalProperties
                .get(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) : null;
        if (autoDdlOption == null)
        {
            autoDdlOption = persistenceUnitMetadata != null ? persistenceUnitMetadata
                    .getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE) : null;
        }
        return autoDdlOption == null ? ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()
                : CassandraConstants.CQL_KEY;
    }
}
