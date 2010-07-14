/**
 * This package contains annotations necessary to qualify an entity object.
 * <br/>
 * <br/>
 * 1. Each entity class must be annotated with @CassandraEntity<br/>
 * 2. Entities of ColumnFamily must be annotated with @ColumnFamily("column-family-name")<br/>
 * 3. Entities of SuperColumnFamily must be annotated with @SuperColumnFamily("super-column-family-name")<br/>
 * 4. Each entity must have a String Field annotated with @Id
 *    
 * @since 0.1
 */
package com.impetus.kundera.api;