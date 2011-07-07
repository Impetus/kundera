/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.db.accessor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.db.accessor.BaseDataAccessor.ThriftRow;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * DataAccessor implementation for Cassandra's ColumnFamily.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public final class ColumnFamilyDataAccessor extends BaseDataAccessor
{

    /** log for this class. */
    private static final Log log = LogFactory.getLog(ColumnFamilyDataAccessor.class);

    /** The Constant TO_ONE_SUPER_COL_NAME. */
    private static final String TO_ONE_SUPER_COL_NAME = "FKey-TO";

    /**
     * Instantiates a new column family data accessor.
     * 
     * @param em
     *            the em
     */
    public ColumnFamilyDataAccessor(EntityManagerImpl em)
    {
        super(em);
    }

    /*
     * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String)
     */
    @Override
    public <E> E read(Class<E> clazz, EntityMetadata m, String id) throws Exception
    {
        log.debug("Column Family >> Read >> " + clazz.getName() + "_" + id);

        String keyspace = m.getSchema();
        String family = m.getTableName();

        return getEntityManager().getClient().loadColumns(getEntityManager(), clazz, keyspace, family, id, m);
    }

    /*
     * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public <E> List<E> read(Class<E> clazz, EntityMetadata m, String... ids) throws Exception
    {
        log.debug("Cassandra >> Read >> " + clazz.getName() + "_(" + Arrays.asList(ids) + ")");

        String keyspace = m.getSchema();
        String family = m.getTableName();

        return getEntityManager().getClient().loadColumns(getEntityManager(), clazz, keyspace, family, m, ids);
    }

    /*
     * @seecom.impetus.kundera.db.DataAccessor#write(com.impetus.kundera.proxy.
     * EnhancedEntity, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public void write(EnhancedEntity e, EntityMetadata m) throws Exception
    {
        String entityName = e.getEntity().getClass().getName();
        String id = e.getId();

        log.debug("Column Family >> Write >> " + entityName + "_" + id);

        getEntityManager().getClient().writeColumns(getEntityManager(), e, m);
    }

    @Override
    public <E> List<E> read(Class<E> clazz, EntityMetadata m, Map<String, String> col) throws Exception
    {
        String keyspace = m.getSchema();
        String family = m.getTableName();
        List<E> entities = new ArrayList<E>();
        for (String superColName : col.keySet())
        {
            String entityId = col.get(superColName);
            List<SuperColumn> map = ((CassandraClient) getEntityManager().getClient()).loadSuperColumns(keyspace,
                    family, entityId, new String[] { superColName });
            E e = fromThriftRow(clazz, m, this.new ThriftRow<SuperColumn>(entityId, family, map));
            entities.add(e);
        }

        return entities;

    }

    /**
     * From thrift row.
     * 
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param cr
     *            the cr
     * @return the e
     * @throws Exception
     *             the exception
     */
    // TODO: this is a duplicate code snippet and we need to refactor this.(Same
    // is with PelopsDataHandler)
    private <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, BaseDataAccessor.ThriftRow<SuperColumn> cr)
            throws Exception
    {

        // Instantiate a new instance
        E e = clazz.newInstance();

        // Set row-key. Note: @Id is always String.
        PropertyAccessorHelper.set(e, m.getIdProperty(), cr.getId());

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        for (Map.Entry<String, EntityMetadata.SuperColumn> entry : m.getSuperColumnsMap().entrySet())
        {
            for (EntityMetadata.Column cMetadata : entry.getValue().getColumns())
            {
                columnNameToFieldMap.put(cMetadata.getName(), cMetadata.getField());
            }
        }

        for (SuperColumn sc : cr.getColumns())
        {

            String scName = PropertyAccessorFactory.STRING.fromBytes(sc.getName());
            boolean intoRelations = false;
            if (scName.equals(TO_ONE_SUPER_COL_NAME))
            {
                intoRelations = true;
            }

            for (Column column : sc.getColumns())
            {
                String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                byte[] value = column.getValue();

                if (value == null)
                {
                    continue;
                }

                if (intoRelations)
                {
                    EntityMetadata.Relation relation = m.getRelation(name);

                    String foreignKeys = PropertyAccessorFactory.STRING.fromBytes(value);
                    Set<String> keys = deserializeKeys(foreignKeys);
                    getEntityManager().getEntityResolver().populateForeignEntities(e, cr.getId(), relation,
                            keys.toArray(new String[0]));

                }
                else
                {
                    // set value of the field in the bean
                    Field field = columnNameToFieldMap.get(name);
                    PropertyAccessorHelper.set(PropertyAccessorHelper.getObject(e, scName), field, value);
                }
            }
        }
        return e;
    }

    // Helper method to convert @Entity to ThriftRow
    /**
     * To thrift row.
     * 
     * @param e
     *            the e
     * @param m
     *            the m
     * @return the base data accessor. thrift row
     * @throws Exception
     *             the exception
     */
    private BaseDataAccessor.ThriftRow<SuperColumn> toThriftRow(EnhancedEntity e, EntityMetadata m) throws Exception
    {

        // timestamp to use in thrift column objects
        long timestamp = System.currentTimeMillis();

        BaseDataAccessor.ThriftRow<SuperColumn> cr = this.new ThriftRow<SuperColumn>();

        // column-family name
        cr.setColumnFamilyName(m.getTableName());

        // Set row key
        cr.setId(e.getId());

        for (EntityMetadata.SuperColumn superColumn : m.getSuperColumnsAsList())
        {
            String superColumnName = superColumn.getName();

            List<Column> columns = new ArrayList<Column>();

            for (EntityMetadata.Column column : superColumn.getColumns())
            {
                Field field = column.getField();
                String name = column.getName();

                try
                {
                    byte[] value = PropertyAccessorHelper.get(e.getEntity(), field);
                    if (null != value)
                    {
                        Column col = new Column();
                        col.setName(PropertyAccessorFactory.STRING.toBytes(name));
                        col.setValue(value);
                        col.setTimestamp(timestamp);
                        columns.add(col);
                    }
                }
                catch (PropertyAccessException exp)
                {
                    log.warn(exp.getMessage());
                }
            }
            SuperColumn superCol = new SuperColumn();
            superCol.setName(PropertyAccessorFactory.STRING.toBytes(superColumnName));
            superCol.setColumns(columns);
            cr.addColumn(superCol);
        }

        // add toOne relations
        List<Column> columns = new ArrayList<Column>();
        for (Map.Entry<String, Set<String>> entry : e.getForeignKeysMap().entrySet())
        {
            String property = entry.getKey();
            Set<String> foreignKeys = entry.getValue();

            String keys = serializeKeys(foreignKeys);
            if (null != keys)
            {
                Column col = new Column();
                col.setName(PropertyAccessorFactory.STRING.toBytes(property));
                col.setValue(PropertyAccessorFactory.STRING.toBytes(keys));
                col.setTimestamp(timestamp);
                columns.add(col);
            }
        }
        if (!columns.isEmpty())
        {
            SuperColumn superCol = new SuperColumn();
            superCol.setName(PropertyAccessorFactory.STRING.toBytes(TO_ONE_SUPER_COL_NAME));
            superCol.setColumns(columns);
            cr.addColumn(superCol);
        }
        return cr;
    }

}
