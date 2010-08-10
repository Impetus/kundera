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

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * DataAccessor implementation for Cassandra's SuperColumnFamily
 * 
 * @author animesh.kumar
 */
public final class SuperColumnFamilyDataAccessor extends BaseDataAccessor<SuperColumn> {

	/** The Constant log. */
    private static final Log log = LogFactory.getLog(SuperColumnFamilyDataAccessor.class);

    private static final String TO_ONE_SUPER_COL_NAME = "FKey-TO";

    public SuperColumnFamilyDataAccessor(EntityManagerImpl em) {
		super(em);
	}

	/*
	 * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
	 * com.impetus.kundera.metadata.EntityMetadata, java.lang.String)
	 */
	@Override
	public <E> E read(Class<E> clazz, EntityMetadata m, String id)
			throws Exception {
		log.debug("Cassandra >> Read >> " + clazz.getName() + "_" + id);

		String keyspace = m.getKeyspaceName();
		String family = m.getColumnFamilyName();

		// get super column names for this entity
        List<String> scNames = m.getSuperColumnFieldNames();
        scNames.add(TO_ONE_SUPER_COL_NAME);
        
        // load column from DB
		List<SuperColumn> columns = getEntityManager().getClient()
				.loadSuperColumns(keyspace, family, id, // row id
						scNames.toArray(new String[0]) // array of names
				);
		
        if (null == columns || columns.size() == 0) {
            throw new PersistenceException("Entity not found for key: " + id);
        }

        E e = fromThriftRow(clazz, m, this.new ThriftRow(id, family, columns));
		return e;
	}

	/*
	 * @see com.impetus.kundera.db.DataAccessor#read(java.lang.Class,
	 * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
	 */
	@Override
	public <E> List<E> read(Class<E> clazz, EntityMetadata m, String... ids)
			throws Exception {
		log.debug("Cassandra >> Read >> " + clazz.getName() + "_("
				+ Arrays.asList(ids) + ")");

		String keyspace = m.getKeyspaceName();
		String family = m.getColumnFamilyName();

		List<E> entities = new ArrayList<E>();

		// load columns from DB
		Map<String, List<SuperColumn>> map = getEntityManager().getClient()
				.loadSuperColumns(keyspace, family, ids);

		// Iterate and populate entities
		for (Map.Entry<String, List<SuperColumn>> entry : map.entrySet()) {

			String id = entry.getKey();
			List<SuperColumn> columns = entry.getValue();

			if (entry.getValue().size() == 0) {
				log.debug("@Entity not found for id: " + id);
				continue;
			}

			E e = fromThriftRow(clazz, m, this.new ThriftRow(id, family,
					columns));
			entities.add(e);
		}

		return entities;
	}

	/*
	 * @seecom.impetus.kundera.db.DataAccessor#write(com.impetus.kundera.proxy.
	 * EnhancedEntity, com.impetus.kundera.metadata.EntityMetadata)
	 */
	@Override
	public void write(EnhancedEntity e, EntityMetadata m) throws Exception {
		String entityName = e.getEntity().getClass().getName();
		String id = e.getId();

		log.debug("Cassandra >> Write >> " + entityName + "_" + id);

		String keyspace = m.getKeyspaceName();
		String family = m.getColumnFamilyName();
		
		BaseDataAccessor<SuperColumn>.ThriftRow tf = toThriftRow(e, m);

		getEntityManager().getClient().writeSuperColumns(keyspace, family, // columnFamily
				tf.getId(), // row id
				tf.getColumns().toArray(new SuperColumn[0]) // list of columns
				);
	}

	// Helper method to convert ThriftRow to @Entity
    private <E> E fromThriftRow (Class<E> clazz, EntityMetadata m,
    		BaseDataAccessor<SuperColumn>.ThriftRow cr) throws Exception {

        // Instantiate a new instance
        E e = clazz.newInstance();

        // Set row-key. Note: @Id is always String.
        PropertyAccessorHelper.set(e, m.getIdProperty(), cr.getId());

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        for (Map.Entry<String, EntityMetadata.SuperColumn> entry : m.getSuperColumnsMap().entrySet()) {
            for (EntityMetadata.Column cMetadata : entry.getValue().getColumns()) {
                columnNameToFieldMap.put(cMetadata.getName(), cMetadata.getField());
            }
        }

        for (SuperColumn sc : cr.getColumns()) {
        	
        	String scName = PropertyAccessorFactory.STRING.fromBytes(sc.getName());
        	boolean intoRelations = false;
        	if (scName.equals(TO_ONE_SUPER_COL_NAME)) {
        		intoRelations = true;
        	}
        	
            for (Column column : sc.getColumns()) {
                String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                byte[] value = column.getValue();

                if (value == null) {
                	continue;
                }
                
				if (intoRelations) {
					EntityMetadata.Relation relation = m.getRelation(name);
										
					String foreignKeys = PropertyAccessorFactory.STRING.fromBytes(value);
					Set<String> keys = deserializeKeys (foreignKeys);
					getEntityManager().populateForeignEntities(e, cr.getId(),
							relation, keys.toArray(new String[0]));


				} else {
					// set value of the field in the bean
					Field field = columnNameToFieldMap.get(name);
					PropertyAccessorHelper.set(e, field, value);
				}
            }
        }
        return e;
    }

	// Helper method to convert @Entity to ThriftRow
    private BaseDataAccessor<SuperColumn>.ThriftRow toThriftRow(EnhancedEntity e, EntityMetadata m) throws Exception {

    	// timestamp to use in thrift column objects
        long timestamp = System.currentTimeMillis();

        BaseDataAccessor<SuperColumn>.ThriftRow cr = this.new ThriftRow();

        // column-family name
        cr.setColumnFamilyName(m.getColumnFamilyName());

        // Set row key
        cr.setId(e.getId());

        for (EntityMetadata.SuperColumn superColumn : m.getSuperColumnsAsList()) {
            String superColumnName = superColumn.getName();

            List<Column> columns = new ArrayList<Column>();

            for (EntityMetadata.Column column : superColumn.getColumns()) {
                Field field = column.getField();
                String name = column.getName();

                try {
                    byte[] value = PropertyAccessorHelper.get(e.getEntity(), field);
                    if (null != value) {
                        columns.add(new Column(PropertyAccessorFactory.STRING.toBytes(name), value, timestamp));
                    }
                } catch (PropertyAccessException exp) {
                    log.warn(exp.getMessage());
                }
            }
            cr.addColumn(new SuperColumn(PropertyAccessorFactory.STRING.toBytes(superColumnName), columns));
        }
        
        
        // add toOne relations
        List<Column> columns = new ArrayList<Column>();
        for (Map.Entry<String, Set<String>> entry : e.getForeignKeysMap().entrySet()) {
        	String property = entry.getKey();
        	Set<String> foreignKeys = entry.getValue();

			String keys = serializeKeys (foreignKeys);
			if (null != keys) {
				columns.add(new Column(PropertyAccessorFactory.STRING
						.toBytes(property), PropertyAccessorFactory.STRING
						.toBytes(keys), timestamp));
			}
        }
        if (!columns.isEmpty()) {
        	cr.addColumn(new SuperColumn(PropertyAccessorFactory.STRING.toBytes(TO_ONE_SUPER_COL_NAME), columns));
        }
        return cr;
    }

}
