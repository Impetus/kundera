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
package com.impetus.client.neo4j;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.neo4j.graphdb.Node;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.DatastoreObjectEntityMapper;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Responsible for converting Neo4J graph (nodes+relationships) to JPA entities and vice versa 
 * @author amresh.singh
 */
public class GraphEntityMapper implements DatastoreObjectEntityMapper
{

    @Override
    public Object fromEntity(Object entity, Object datastoreObject, List<RelationHolder> relations, EntityMetadata m)
    {
        Node node = (Node) datastoreObject;
        
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        Set<Attribute> attributes = entityType.getAttributes();   
        
        
        for(Attribute attribute : attributes)
        {
            if(! attribute.isCollection() && ! attribute.isAssociation())
            {
                Field field = (Field)attribute.getJavaMember();                
                String columnName = ((AbstractAttribute)attribute).getJPAColumnName();
                Object value = PropertyAccessorHelper.getObject(entity, field);
                
                node.setProperty(columnName, value);
                
            }
        }
        
        return node;
    }

    @Override
    public Object toEntity(Object datastoreObject, List<String> relationNames, EntityMetadata m)
    {
        return null;
    }

}
