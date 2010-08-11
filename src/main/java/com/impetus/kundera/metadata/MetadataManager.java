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
package com.impetus.kundera.metadata;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.EntityManagerFactory;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.classreading.AnnotationDiscoveryListener;
import com.impetus.kundera.metadata.EntityMetadata.Relation;
import com.impetus.kundera.metadata.processor.CacheableAnnotationProcessor;
import com.impetus.kundera.metadata.processor.ColumnFamilyProcessor;
import com.impetus.kundera.metadata.processor.EntityListenersProcessor;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.processor.SuperColumnFamilyProcessor;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Concrete implementation of IMetadataManager.
 * 
 * @author animesh.kumar
 */
public class MetadataManager implements AnnotationDiscoveryListener {

	/** the log used by this class. */
	private static Log log = LogFactory.getLog(MetadataManager.class);

	/** cache for Metadata. */
	private Map<Class<?>, EntityMetadata> metadataCache = new ConcurrentHashMap<Class<?>, EntityMetadata>();

	/** The entity name to class map. */
	private Map<String, Class<?>> entityNameToClassMap = new ConcurrentHashMap<String, Class<?>>();

	/** The metadata processors. */
	private List<MetadataProcessor> metadataProcessors;

	/** The Validator. */
	private Validator validator;

	// intentionally unused!
	/** The factory. */
	@SuppressWarnings("unused")
	private EntityManagerFactory factory;

	// set after build is called?
	/** The instantiated. */
	private boolean instantiated = false;

	/**
	 * Instantiates a new metadata manager.
	 * 
	 * @param factory
	 *            the factory
	 */
	public MetadataManager(EntityManagerFactory factory) {
		this.factory = factory;

		validator = new ValidatorImpl();

		metadataProcessors = new ArrayList<MetadataProcessor>();

		// add processors to chain.
		metadataProcessors.add(new SuperColumnFamilyProcessor(factory));
		metadataProcessors.add(new ColumnFamilyProcessor(factory));
		metadataProcessors.add(new CacheableAnnotationProcessor());
		metadataProcessors.add(new IndexProcessor());
		metadataProcessors.add(new EntityListenersProcessor());
	}

	/**
	 * Validate.
	 * 
	 * @param clazz
	 *            the clazz
	 * 
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	public final void validate(Class<?> clazz) throws PersistenceException {
		validator.validate(clazz);
	}

	/**
	 * Checks if is column family.
	 * 
	 * @param clazz
	 *            the clazz
	 * 
	 * @return true, if is column family
	 * 
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	public final boolean isColumnFamily(Class<?> clazz) {
		return getEntityMetadata(clazz).getType().equals(
				EntityMetadata.Type.COLUMN_FAMILY);
	}

	/**
	 * Checks if is super column family.
	 * 
	 * @param clazz
	 *            the clazz
	 * 
	 * @return true, if is super column family
	 * 
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	public final boolean isSuperColumnFamily(Class<?> clazz) {
		return getEntityMetadata(clazz).getType().equals(
				EntityMetadata.Type.SUPER_COLUMN_FAMILY);
	}

	/**
	 * Gets the entity metadata.
	 * 
	 * @param clazz
	 *            the clazz
	 * 
	 * @return the entity metadata
	 * 
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	public final EntityMetadata getEntityMetadata(Class<?> clazz) {

		EntityMetadata metadata = metadataCache.get(clazz);
		if (null == metadata) {
			log.debug("Metadata not found in cache for " + clazz.getName());
			// double check locking.
			synchronized (clazz) {
				if (null == metadata) {
					metadata = process(clazz);
					cacheMetadata(clazz, metadata);
				}
			}
		}
		return metadata;
	}

	// helper methods to strip CGLIB from class

	/**
	 * Process.
	 * 
	 * @param clazz
	 *            the clazz
	 * 
	 * @return the entity metadata
	 * 
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	private EntityMetadata process(Class<?> clazz) {

		EntityMetadata metadata = new EntityMetadata(clazz);
		validate(clazz);

		log.debug("Processing @Entity: " + clazz);

		for (MetadataProcessor processor : metadataProcessors) {
			processor.process(clazz, metadata);
		}

		return metadata;
	}

	/**
	 * Cache metadata.
	 * 
	 * @param clazz
	 *            the clazz
	 * @param metadata
	 *            the metadata
	 */
	private void cacheMetadata(Class<?> clazz, EntityMetadata metadata) {
		metadataCache.put(clazz, metadata);

		// save name to class map.
		if (entityNameToClassMap.containsKey(clazz.getSimpleName())) {
			throw new PersistenceException("Name conflict between classes "
					+ entityNameToClassMap.get(clazz.getSimpleName()).getName()
					+ " and " + clazz.getName());
		}
		entityNameToClassMap.put(clazz.getSimpleName(), clazz);
	}

	/**
	 * Gets the entity class by name.
	 * 
	 * @param name
	 *            the name
	 * 
	 * @return the entity class by name
	 */
	public final Class<?> getEntityClassByName(String name) {
		return entityNameToClassMap.get(name);
	}

	/**
	 * Gets the entity metadatas as list.
	 * 
	 * @return the entity metadatas as list
	 */
	public final List<EntityMetadata> getEntityMetadatasAsList() {
		return Collections.unmodifiableList(new ArrayList<EntityMetadata>(
				metadataCache.values()));
	}

	/*
	 * @see
	 * com.impetus.kundera.classreading.AnnotationDiscoveryListener#discovered
	 * (java.lang.String, java.lang.String[])
	 */
	@Override
	// called whenever a class with @Entity annotation is encountered in the
	// classpath.
	public final void discovered(String className, String[] annotations) {
		try {
			Class<?> clazz = Class.forName(className);

			// process for Metadata
			EntityMetadata metadata = process(clazz);
			cacheMetadata(clazz, metadata);
			log.info("Added @Entity " + clazz.getName());
		} catch (ClassNotFoundException e) {
			throw new PersistenceException(e.getMessage());
		}
	}

	/**
	 * Build Inter/Intra @Entity relationships.
	 */
	public void build() {
		for (EntityMetadata metadata : getEntityMetadatasAsList()) {
			processRelations(metadata.getEntityClazz());
			log.debug("Metadata for @Entity " + metadata.getEntityClazz()
					+ "\n" + metadata);
		}
		instantiated = true;
	}

	// Helper class to scan each @Entity class and build various
	// relational annotation.
	/**
	 * Process relations.
	 * 
	 * @param entity
	 *            the entity
	 */
	private void processRelations(Class<?> entity) {
		EntityMetadata metadata = getEntityMetadata(entity);

		for (Field f : entity.getDeclaredFields()) {

			// OneToOne
			if (f.isAnnotationPresent(OneToOne.class)) {
				// taking field's type as foreign entity, ignoring
				// "targetEntity"
				Class<?> targetEntity = f.getType();
				try {
					validate(targetEntity);
					OneToOne ann = f.getAnnotation(OneToOne.class);

					Relation relation = metadata.new Relation(f, targetEntity,
							null, ann.fetch(), Arrays.asList(ann.cascade()),
							ann.optional(), ann.mappedBy(),
							EntityMetadata.ForeignKey.ONE_TO_ONE);

					metadata.addRelation(f.getName(), relation);
				} catch (PersistenceException pe) {
					throw new PersistenceException(
							"Error with @OneToOne in @Entity("
									+ entity.getName() + "." + f.getName()
									+ "), reason: " + pe.getMessage());
				}
			}

			// OneToMany
			else if (f.isAnnotationPresent(OneToMany.class)) {

				OneToMany ann = f.getAnnotation(OneToMany.class);

				Class<?> targetEntity = null;

				// resolve from generic parameters
				Type[] parameters = ReflectUtils.getTypeArguments(f);
				if (parameters != null) {
					if (parameters.length == 1) {
						targetEntity = (Class<?>) parameters[0];
					} else {
						throw new PersistenceException(
								"How many parameters man?");
					}
				}
				// now, check annotations
				if (null != ann.targetEntity()
						&& !ann.targetEntity().getSimpleName().equals("void")) {
					targetEntity = ann.targetEntity();
				}

				try {
					validate(targetEntity);
					Relation relation = metadata.new Relation(f, targetEntity,
							f.getType(), ann.fetch(), Arrays.asList(ann
									.cascade()), Boolean.TRUE, ann.mappedBy(),
							EntityMetadata.ForeignKey.ONE_TO_MANY);

					metadata.addRelation(f.getName(), relation);
				} catch (PersistenceException pe) {
					throw new PersistenceException(
							"Error with @OneToMany in @Entity("
									+ entity.getName() + "." + f.getName()
									+ "), reason: " + pe.getMessage());
				}
			}

			// ManyToOne
			else if (f.isAnnotationPresent(ManyToOne.class)) {
				// taking field's type as foreign entity, ignoring
				// "targetEntity"
				Class<?> targetEntity = f.getType();
				try {
					validate(targetEntity);
					ManyToOne ann = f.getAnnotation(ManyToOne.class);

					Relation relation = metadata.new Relation(f, targetEntity,
							null, ann.fetch(), Arrays.asList(ann.cascade()),
							ann.optional(), null, // mappedBy is null
							EntityMetadata.ForeignKey.MANY_TO_ONE);

					metadata.addRelation(f.getName(), relation);
				} catch (PersistenceException pe) {
					throw new PersistenceException(
							"Error with @OneToOne in @Entity("
									+ entity.getName() + "." + f.getName()
									+ "), reason: " + pe.getMessage());
				}
			}

			// ManyToMany
			else if (f.isAnnotationPresent(ManyToMany.class)) {

				ManyToMany ann = f.getAnnotation(ManyToMany.class);

				Class<?> targetEntity = null;

				// resolve from generic parameters
				Type[] parameters = ReflectUtils.getTypeArguments(f);
				if (parameters != null) {
					if (parameters.length == 1) {
						targetEntity = (Class<?>) parameters[0];
					} else {
						throw new PersistenceException(
								"How many parameters man?");
					}
				}
				// now, check annotations
				if (null != ann.targetEntity()
						&& !ann.targetEntity().getSimpleName().equals("void")) {
					targetEntity = ann.targetEntity();
				}

				try {
					validate(targetEntity);
					Relation relation = metadata.new Relation(f, targetEntity,
							f.getType(), ann.fetch(), Arrays.asList(ann
									.cascade()), Boolean.TRUE, ann.mappedBy(),
							EntityMetadata.ForeignKey.MANY_TO_MANY);

					metadata.addRelation(f.getName(), relation);
				} catch (PersistenceException pe) {
					throw new PersistenceException(
							"Error with @OneToMany in @Entity("
									+ entity.getName() + "." + f.getName()
									+ "), reason: " + pe.getMessage());
				}
			}

		}
	}
}
