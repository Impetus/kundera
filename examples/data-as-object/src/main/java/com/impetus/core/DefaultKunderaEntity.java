package com.impetus.core;

import javax.persistence.EntityManager;

import com.impetus.dao.PersistenceService;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.processor.TableProcessor;

public abstract class DefaultKunderaEntity<T, K> implements KunderaEntity<T, K> {
	private EntityManager em = PersistenceService.getEM();
	public final T find(K key) {
		
		EntityMetadata metadata = new EntityMetadata(this.getClass());
		new TableProcessor(null, null).process(this.getClass(), metadata);
		((MetamodelImpl)em.getMetamodel()).addEntityMetadata(this.getClass(), metadata);
		em.find(this.getClass(), em.getMetamodel().entity(this.getClass())
				.getId(key.getClass()));
		return null;
	}

	public final void save() {
		em.persist(this);
	}

	public final void update() {
		em.merge(this);
	}

	public final void delete() {
		em.remove(this);
	}

}
