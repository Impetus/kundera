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
package com.impetus.kundera.proxy;

import javax.persistence.PersistenceException;

import com.impetus.kundera.ejb.EntityManagerImpl;

/**
 * Handles fetching of the underlying entity for a proxy
 * 
 * @author Gavin King
 * @author Steve Ebersole
 */
public interface LazyInitializer {
	/**
	 * Initialize the proxy, fetching the target entity if necessary.
	 * 
	 */
	public void initialize() throws PersistenceException;

	/**
	 * Retrieve the identifier value for the enity our owning proxy represents.
	 * 
	 * @return The identifier value.
	 */
	public String getIdentifier();

	/**
	 * Set the identifier value for the enity our owning proxy represents.
	 * 
	 * @param id
	 *            The identifier value.
	 */
	public void setIdentifier(String id);

	/**
	 * The entity-name of the entity our owning proxy represents.
	 * 
	 * @return The entity-name.
	 */
	public String getEntityName();

	/**
	 * Get the actual class of the entity. Generally, {@link #getEntityName()}
	 * should be used instead.
	 * 
	 * @return The actual entity class.
	 */
	public Class<?> getPersistentClass();

	/**
	 * Is the proxy uninitialzed?
	 * 
	 * @return True if uninitialized; false otherwise.
	 */
	public boolean isUninitialized();

	/**
	 * Get the session to which this proxy is associated, or null if it is not
	 * attached.
	 * 
	 * @return The associated session.
	 */
	public EntityManagerImpl getEntityManager ();

	/**
	 * Unset this initializer's reference to session. It is assumed that the
	 * caller is also taking care or cleaning up the owning proxy's reference in
	 * the persistence context.
	 * <p/>
	 * Generally speaking this is intended to be called only during
	 * {@link org.hibernate.Session#evict} and
	 * {@link org.hibernate.Session#clear} processing; most other use-cases
	 * should call {@link #setSession} instead.
	 */
	public void unsetEntityManager();

	public void setUnwrap(boolean unwrap);

	public boolean isUnwrap();
}
