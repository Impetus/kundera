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
import javax.persistence.ManyToMany;
import javax.persistence.PersistenceException;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.processor.AbstractEntityFieldProcessor;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author Amresh Singh
 */
public class ManyToManyRelationMetadataProcessor extends AbstractEntityFieldProcessor implements RelationMetadataProcessor {
	
	public ManyToManyRelationMetadataProcessor() {
		validator = new EntityValidatorImpl();
	}

	@Override
	public void addRelationIntoMetadata(Field relationField, EntityMetadata metadata) {
		ManyToMany ann = relationField.getAnnotation(ManyToMany.class);

		Class<?> targetEntity = PropertyAccessorHelper
				.getGenericClass(relationField);
		// now, check annotations
		if (null != ann.targetEntity()
				&& !ann.targetEntity().getSimpleName().equals("void")) {
			targetEntity = ann.targetEntity();
		}

		validate(targetEntity);
		Relation relation = new Relation(relationField, targetEntity,
				relationField.getType(), ann.fetch(), Arrays.asList(ann
						.cascade()), Boolean.TRUE, ann.mappedBy(),
				Relation.ForeignKey.MANY_TO_MANY);

		boolean isJoinedByFK = relationField.isAnnotationPresent(JoinColumn.class);
        boolean isJoinedByTable = relationField.isAnnotationPresent(JoinTable.class);
        
        if(isJoinedByFK) {
        	throw new PersistenceException("@JoinColumn not allowed for ManyToMany relationship. Use @JoinTable instead");
        	
        } else if(isJoinedByTable) {
        	JoinTableMetadata jtMetadata = new JoinTableMetadata(relationField);
        	
        	relation.setRelatedViaJoinTable(true);
        	relation.setJoinTableMetadata(jtMetadata);  
        } else {
        	throw new PersistenceException("It's manadatory to use @JoinTable for a ManyToMany relationship.");
        }	
		
		metadata.addRelation(relationField.getName(), relation);
		
	}

	@Override
	public void process(Class<?> clazz, EntityMetadata metadata) {

		
	}	
	
}
