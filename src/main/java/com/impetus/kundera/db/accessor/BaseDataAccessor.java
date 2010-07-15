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

import java.util.ArrayList;
import java.util.List;

import com.impetus.kundera.metadata.EntityMetadata;

/**
 * The Class BaseDataAccessor.
 * 
 * @param <TF>
 *            represents cassandra data unit. could be, Column or SuperColumn
 * 
 * @author animesh.kumar
 */
public abstract class BaseDataAccessor<TF> {

    /**
     * converts Entity to CassandraRow.
     * 
     * @param metadata
     *            Entity metadata
     * @param entity
     *            Entity
     * 
     * @return BaseDataAccessor<TF>.CassandraRow
     * 
     * @throws Exception
     *             the exception
     */
    protected abstract CassandraRow entityToCassandraRow(EntityMetadata metadata, Object entity) throws Exception;

    /**
     * converts CassandraRow to Entity.
     * 
     * @param clazz
     *            Entity class
     * @param metadata
     *            Entity metadata
     * @param cassandraRow
     *            BaseDataAccessor<TF>.CassandraRow object
     * 
     * @return Entity
     * 
     * @throws Exception
     *             the exception
     */
    protected abstract <C> C cassandraRowToEntity(Class<C> clazz, EntityMetadata metadata, CassandraRow cassandraRow) throws Exception;

    /**
     * utility class to bridge the gap between thrift and entity. It represents
     * a row from of Columns or SuperColumns.
     * 
     * @author animesh.kumar
     */
    public class CassandraRow {

        /** key of the row. */
        private String key;

        /** name of the family. */
        private String columnFamilyName;

        /** list of thrift columns from the row. */
        private List<TF> columns;

        /**
         * default constructor.
         */
        public CassandraRow() {
            columns = new ArrayList<TF>();
        }

        /**
         * The Constructor.
         * 
         * @param key
         *            the key
         * @param columnFamilyName
         *            the column family name
         * @param columns
         *            the columns
         */
        public CassandraRow(String key, String columnFamilyName, List<TF> columns) {
            super();
            this.key = key;
            this.columnFamilyName = columnFamilyName;
            this.columns = columns;
        }

        /**
         * Gets the key.
         * 
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Sets the key.
         * 
         * @param key
         *            the key to set
         */
        public void setKey(String key) {
            this.key = key;
        }

        /**
         * Gets the column family name.
         * 
         * @return the columnFamilyName
         */
        public String getColumnFamilyName() {
            return columnFamilyName;
        }

        /**
         * Sets the column family name.
         * 
         * @param columnFamilyName
         *            the columnFamilyName to set
         */
        public void setColumnFamilyName(String columnFamilyName) {
            this.columnFamilyName = columnFamilyName;
        }

        /**
         * Gets the columns.
         * 
         * @return the columns
         */
        public List<TF> getColumns() {
            return columns;
        }

        /**
         * Sets the columns.
         * 
         * @param columns
         *            the columns to set
         */
        public void setColumns(List<TF> columns) {
            this.columns = columns;
        }

        /**
         * Adds the column.
         * 
         * @param column
         *            the column
         */
        public void addColumn(TF column) {
            columns.add(column);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("CassandraRow [key=");
            builder.append(key);
            builder.append(", columnFamilyName=");
            builder.append(columnFamilyName);
            builder.append(", columns=");
            builder.append(columns);
            builder.append("]");
            return builder.toString();
        }

    }

}
