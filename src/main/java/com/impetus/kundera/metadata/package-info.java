/**
 * Cassandra supports 2 types of data models: Column and SuperColumn. Broadly speaking,
 * Column is tuple that contains Name, Value and a Timestamp; and SuperColumn is a Column 
 * of Columns. Cassandra stores its data models in families: Column in ColumnFamily and 
 * SuperColumn in SuperColumnFamily. 
 * <br/>
 * <br/>
 * Kundera has @SuperColumnFamily annotation to bind the annotated entity to SuperColumnFamily, 
 * and @ColumnFamily to ColumnFamily. This package contains classes to pick up classes with 
 * these annotations. Kundera defines {@link ColumMetadata} to store meta information of entity classes annotated
 * with {@link ColumnFamily}, and {@link SuperColumnMetadata} for {@link SuperColumn}
 * <br/>
 * <br/>
 * Outside world can get a hold of @{link IMetadataManager}
 *  
 *    
 * @since 0.1
 */
package com.impetus.kundera.metadata;