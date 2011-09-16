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
package com.impetus.kundera.metadata.processor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import javax.persistence.CollectionTable;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PersistenceException;
import javax.persistence.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Metadata processor class for persistent entities
 * 
 * @author amresh.singh
 */
public class TableProcessor extends AbstractEntityFieldProcessor {

	/** The Constant log. */
	private static final Log LOG = LogFactory.getLog(TableProcessor.class);

	public TableProcessor() {
		validator = new EntityValidatorImpl();
	}

	@Override
	public void process(Class<?> clazz, EntityMetadata metadata) {
		if (!clazz.isAnnotationPresent(Table.class)) {
			return;
		}

		LOG.debug("Processing @Entity(" + clazz.getName()
				+ ") for Persistence Object.");
		populateMetadata(metadata, clazz);
	}

	private void populateMetadata(EntityMetadata metadata, Class<?> clazz) {
		Table table = clazz.getAnnotation(Table.class);
		boolean isEmbeddable = false;
		// Set Name of persistence object
		metadata.setTableName(table.name());

		// set schema name and persistence unit name (if provided)
		String schemaStr = table.schema();
		if (schemaStr == null) {
			LOG.error("It is mandatory to specify Schema alongwith Table name:"
					+ table.name() + ". This entity won't be persisted");
			throw new PersistenceException(
					"It is mandatory to specify Schema alongwith Table name:"
							+ table.name() + ". This entity won't be persisted");
		}
		MetadataUtils.setSchemaAndPersistenceUnit(metadata, schemaStr);

		metadata.setType(com.impetus.kundera.metadata.model.EntityMetadata.Type.COLUMN_FAMILY);
		// scan for fields

		for (Field f : clazz.getDeclaredFields()) {
			/* Scan @Id field */
			if (f.isAnnotationPresent(Id.class)) {
				LOG.debug(f.getName() + " => Id");
				populateIdAccessorMethods(metadata, clazz, f);
				populateIdColumn(metadata, clazz, f);
			}

			else if (f.isAnnotationPresent(Embedded.class)) {
				/* Scan @Embedded fields */
				metadata.setType(com.impetus.kundera.metadata.model.EntityMetadata.Type.SUPER_COLUMN_FAMILY);
				Class embeddedFieldClass = f.getType();
				isEmbeddable = true;

				if (Collection.class.isAssignableFrom(embeddedFieldClass)) {
					LOG.warn(f.getName()
							+ " was annotated with @Embedded, and shouldn't have been a java Collection field, it won't be persisted");
				} else {
					// An @Embedded attribute will be a DTO (@Embeddable)
					populateEmbeddedFieldIntoMetadata(metadata, f,
							embeddedFieldClass);
					/* TODO: Bad code, see how to remove this */
					metadata.addToEmbedCollection(embeddedFieldClass);
				}

			}

			else if (f.isAnnotationPresent(ElementCollection.class)) {
				/* Scan @ElementCollection fields */
				metadata.setType(com.impetus.kundera.metadata.model.EntityMetadata.Type.SUPER_COLUMN_FAMILY);
				Class elementCollectionFieldClass = f.getType();
				isEmbeddable = true;

				// An @ElementCollection must be a java collection, a generic
				// class must be declared (@Embeddable)
				if (Collection.class
						.isAssignableFrom(elementCollectionFieldClass)) {
					Class elementCollectionGenericClass = PropertyAccessorHelper
							.getGenericClass(f);
					populateElementCollectionIntoMetadata(metadata, f,
							elementCollectionGenericClass);

					/* TODO: Bad code, see how to remove this */
					metadata.addToEmbedCollection(elementCollectionGenericClass);
				} else {
					LOG.warn(f.getName()
							+ " was annotated with @ElementCollection but wasn't a java Collection field, it won't be persisted");
				}
			} else {
				/* if any valid JPA annotation? */
				String name = getValidJPAColumnName(clazz, f);
				if (null != name) {
					// additional check for not to load Unnecessary column
					// objects in JVM.
					if (!isEmbeddable) {
						metadata.addColumn(name,
								new com.impetus.kundera.metadata.model.Column(
										name, f));
					}
					EmbeddedColumn embeddedColumn = new EmbeddedColumn(name, f);
					metadata.addEmbeddedColumn(name, embeddedColumn);
					embeddedColumn.addColumn(name, f);
				}
			}

			/* Scan for Relationship field */
			addRelationIntoMetadata(clazz, f, metadata);
		}

		// TODO: Below if/else block is possibly not required, should be removed
		if (isEmbeddable) {
			Map<String, com.impetus.kundera.metadata.model.Column> cols = metadata
					.getColumnsMap();
			cols.clear();
			cols = null;
			metadata.setType(Type.SUPER_COLUMN_FAMILY);
		} else {
			Map<String, EmbeddedColumn> embeddedColumns = metadata
					.getEmbeddedColumnsMap();
			embeddedColumns.clear();
			embeddedColumns = null;
			metadata.setType(Type.COLUMN_FAMILY);
		}

	}

	private void populateEmbeddedFieldIntoMetadata(EntityMetadata metadata,
			Field embeddedField, Class embeddedFieldClass) {
		// If not annotated with @Embeddable, discard.
		Annotation ann = embeddedFieldClass.getAnnotation(Embeddable.class);
		if (ann == null) {
			LOG.warn(embeddedField.getName()
					+ " was declared @Embedded but wasn't annotated with @Embeddable. "
					+ " It won't be persisted co-located.");
			// return;
		}

		// TODO: Provide user an option to specify this in entity class rather
		// than default field name
		String embeddedFieldName = embeddedField.getName();
		addEmbeddedColumnInMetadata(metadata, embeddedField,
				embeddedFieldClass, embeddedFieldName);
	}

	private void populateElementCollectionIntoMetadata(EntityMetadata metadata,
			Field embeddedField, Class embeddedFieldClass) {
		// If not annotated with @Embeddable, discard.
		Annotation ann = embeddedFieldClass.getAnnotation(Embeddable.class);
		if (ann == null) {
			LOG.warn(embeddedField.getName()
					+ " was declared @ElementCollection but wasn't annotated with @Embeddable. "
					+ " It won't be persisted co-located.");
			// return;
		}

		String embeddedFieldName = null;

		// Embedded object name should be the one provided with @CollectionTable
		// annotation, if not, should be
		// equal to field name
		if (embeddedField.isAnnotationPresent(CollectionTable.class)) {
			CollectionTable ct = embeddedField
					.getAnnotation(CollectionTable.class);
			if (!ct.name().isEmpty()) {
				embeddedFieldName = ct.name();
			} else {
				embeddedFieldName = embeddedField.getName();
			}
		} else {
			embeddedFieldName = embeddedField.getName();
		}

		addEmbeddedColumnInMetadata(metadata, embeddedField,
				embeddedFieldClass, embeddedFieldName);
	}

	/**
	 * TODO: Change method name once we change the name "Super Column" in entity
	 * metadata
	 * 
	 * @param metadata
	 * @param embeddedField
	 * @param embeddedFieldClass
	 * @param embeddedFieldName
	 */
	private void addEmbeddedColumnInMetadata(EntityMetadata metadata,
			Field embeddedField, Class embeddedFieldClass,
			String embeddedFieldName) {
		EmbeddedColumn superColumn = metadata
				.getEmbeddedColumn(embeddedFieldName);
		if (null == superColumn) {
			superColumn = new EmbeddedColumn(embeddedFieldName, embeddedField);
		}
		// Iterate over all fields of this super column class
		for (Field columnField : embeddedFieldClass.getDeclaredFields()) {
			String columnName = getValidJPAColumnName(embeddedFieldClass,
					columnField);
			if (columnName == null) {
				columnName = columnField.getName();
			}
			superColumn.addColumn(columnName, columnField);
		}
		metadata.addEmbeddedColumn(embeddedFieldName, superColumn);
	}

	/**
	 * Adds relationship info into metadata for a given field
	 * <code>relationField</code>
	 */
	private void addRelationIntoMetadata(Class<?> entity, Field relationField,
			EntityMetadata metadata) {

		// OneToOne
		if (relationField.isAnnotationPresent(OneToOne.class)) {
			// taking field's type as foreign entity, ignoring
			// "targetEntity"
			Class<?> targetEntity = relationField.getType();
			try {
				// TODO: Add code to check whether this entity has already been
				// validated, at all placed below
				validate(targetEntity);
				OneToOne ann = relationField.getAnnotation(OneToOne.class);

				Relation relation = new Relation(relationField, targetEntity,
						null, ann.fetch(), Arrays.asList(ann.cascade()),
						ann.optional(), ann.mappedBy(),
						Relation.ForeignKey.ONE_TO_ONE);

				metadata.addRelation(relationField.getName(), relation);
			} catch (PersistenceException pe) {
				throw new PersistenceException(
						"Error with @OneToOne in @Entity(" + entity + "."
								+ relationField.getName() + "), reason: "
								+ pe.getMessage());
			}
		}

		// OneToMany
		else if (relationField.isAnnotationPresent(OneToMany.class)) {

			OneToMany ann = relationField.getAnnotation(OneToMany.class);

			Class<?> targetEntity = PropertyAccessorHelper
					.getGenericClass(relationField);

			// now, check annotations
			if (null != ann.targetEntity()
					&& !ann.targetEntity().getSimpleName().equals("void")) {
				targetEntity = ann.targetEntity();
			}

			try {
				validate(targetEntity);
				Relation relation = new Relation(relationField, targetEntity,
						relationField.getType(), ann.fetch(), Arrays.asList(ann
								.cascade()), Boolean.TRUE, ann.mappedBy(),
						Relation.ForeignKey.ONE_TO_MANY);

				metadata.addRelation(relationField.getName(), relation);
			} catch (PersistenceException pe) {
				throw new PersistenceException(
						"Error with @OneToMany in @Entity(" + entity.getName()
								+ "." + relationField.getName() + "), reason: "
								+ pe.getMessage());
			}
		}

		// ManyToOne
		else if (relationField.isAnnotationPresent(ManyToOne.class)) {
			// taking field's type as foreign entity, ignoring
			// "targetEntity"
			Class<?> targetEntity = relationField.getType();
			try {
				validate(targetEntity);
				ManyToOne ann = relationField.getAnnotation(ManyToOne.class);

				Relation relation = new Relation(relationField, targetEntity,
						null, ann.fetch(), Arrays.asList(ann.cascade()),
						ann.optional(), null, // mappedBy is
												// null
						Relation.ForeignKey.MANY_TO_ONE);

				metadata.addRelation(relationField.getName(), relation);
			} catch (PersistenceException pe) {
				throw new PersistenceException(
						"Error with @OneToOne in @Entity(" + entity.getName()
								+ "." + relationField.getName() + "), reason: "
								+ pe.getMessage());
			}
		}

		// ManyToMany
		else if (relationField.isAnnotationPresent(ManyToMany.class)) {

			ManyToMany ann = relationField.getAnnotation(ManyToMany.class);

			Class<?> targetEntity = PropertyAccessorHelper
					.getGenericClass(relationField);
			// now, check annotations
			if (null != ann.targetEntity()
					&& !ann.targetEntity().getSimpleName().equals("void")) {
				targetEntity = ann.targetEntity();
			}

			try {
				validate(targetEntity);
				Relation relation = new Relation(relationField, targetEntity,
						relationField.getType(), ann.fetch(), Arrays.asList(ann
								.cascade()), Boolean.TRUE, ann.mappedBy(),
						Relation.ForeignKey.MANY_TO_MANY);

				metadata.addRelation(relationField.getName(), relation);
			} catch (PersistenceException pe) {
				throw new PersistenceException(
						"Error with @OneToMany in @Entity(" + entity.getName()
								+ "." + relationField.getName() + "), reason: "
								+ pe.getMessage());
			}
		}

	}

}
