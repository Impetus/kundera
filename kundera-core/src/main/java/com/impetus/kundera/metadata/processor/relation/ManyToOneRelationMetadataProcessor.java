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
package com.impetus.kundera.metadata.processor.relation;

import java.lang.reflect.Field;
import java.util.Arrays;

import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.processor.AbstractEntityFieldProcessor;

/**
 * @author Amresh Singh
 */
public class ManyToOneRelationMetadataProcessor extends AbstractEntityFieldProcessor implements RelationMetadataProcessor {


	@Override
	public void addRelationIntoMetadata(Field relationField, EntityMetadata metadata) {
		// taking field's type as foreign entity, ignoring  "targetEntity"
		Class<?> targetEntity = relationField.getType();

		validate(targetEntity);
		
		ManyToOne ann = relationField.getAnnotation(ManyToOne.class);

		Relation relation = new Relation(relationField, targetEntity,
				null, ann.fetch(), Arrays.asList(ann.cascade()),
				ann.optional(), null, // mappedBy is
										// null
				Relation.ForeignKey.MANY_TO_ONE);

		boolean isJoinedByFK = relationField.isAnnotationPresent(JoinColumn.class);
        boolean isJoinedByTable = relationField.isAnnotationPresent(JoinTable.class);
        
        if(isJoinedByFK) {
        	JoinColumn joinColumnAnn = relationField.getAnnotation(JoinColumn.class);
        	relation.setJoinColumnName(joinColumnAnn.name());
        } else if(isJoinedByTable) {
        	JoinTableMetadata jtMetadata = new JoinTableMetadata(relationField);
        	
        	relation.setRelatedViaJoinTable(true);
        	relation.setJoinTableMetadata(jtMetadata);  
        }        
		
		metadata.addRelation(relationField.getName(), relation);
		
	}

	@Override
	public void process(Class<?> clazz, EntityMetadata metadata) {

		
	}
	

}
