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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.CallbackFilter;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.InvocationHandler;
import net.sf.cglib.proxy.NoOp;

import com.impetus.kundera.LazyInitializationException;
import com.impetus.kundera.ejb.EntityManagerImpl;

/**
 * A <tt>LazyInitializer</tt> implemented using the CGLIB bytecode generation
 * library.
 */
public final class CglibLazyInitializer implements LazyInitializer,
		InvocationHandler {

	/** The Constant log. */
	private static final Log log = LogFactory
			.getLog(CglibLazyInitializer.class);

	/** The entity name. */
	private String entityName;

	/** The id. */
	private String id;

	/** The target. */
	private Object target;

	/** The initialized. */
	private boolean initialized;

	/** The unwrap. */
	private boolean unwrap;

	/** The persistent class. */
	protected Class<?> persistentClass;

	/** The get identifier method. */
	protected Method getIdentifierMethod;

	/** The set identifier method. */
	protected Method setIdentifierMethod;

	/** The interfaces. */
	private Class<?>[] interfaces;

	/** The constructed. */
	private boolean constructed = false;

	/** The em. */
	private transient EntityManagerImpl em;

	/** The Constant FINALIZE_FILTER. */
	private static final CallbackFilter FINALIZE_FILTER = new CallbackFilter() {
		public int accept(Method method) {
			if (method.getParameterTypes().length == 0
					&& method.getName().equals("finalize")) {
				return 1;
			} else {
				return 0;
			}
		}
	};

	/**
	 * Gets the proxy.
	 * 
	 * @param entityName
	 *            the entity name
	 * @param persistentClass
	 *            the persistent class
	 * @param interfaces
	 *            the interfaces
	 * @param getIdentifierMethod
	 *            the get identifier method
	 * @param setIdentifierMethod
	 *            the set identifier method
	 * @param id
	 *            the id
	 * @param em
	 *            the em
	 * @return the proxy
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	public static KunderaProxy getProxy(final String entityName,
			final Class<?> persistentClass, final Class<?>[] interfaces,
			final Method getIdentifierMethod, final Method setIdentifierMethod,
			final String id, final EntityManagerImpl em)
			throws PersistenceException {

		try {
			final CglibLazyInitializer instance = new CglibLazyInitializer(
					entityName, persistentClass, interfaces, id,
					getIdentifierMethod, setIdentifierMethod, em);

			final KunderaProxy proxy;
			Class factory = getProxyFactory(persistentClass, interfaces);
			proxy = getProxyInstance(factory, instance);
			instance.constructed = true;
			return proxy;
		} catch (Throwable t) {
			throw new PersistenceException("CGLIB Enhancement failed: "
					+ entityName, t);
		}
	}

	/**
	 * Gets the proxy instance.
	 * 
	 * @param factory
	 *            the factory
	 * @param instance
	 *            the instance
	 * @return the proxy instance
	 * @throws InstantiationException
	 *             the instantiation exception
	 * @throws IllegalAccessException
	 *             the illegal access exception
	 */
	private static KunderaProxy getProxyInstance(Class factory,
			CglibLazyInitializer instance) throws InstantiationException,
			IllegalAccessException {
		KunderaProxy proxy;
		try {
			Enhancer.registerCallbacks(factory,
					new Callback[] { instance, null });
			proxy = (KunderaProxy) factory.newInstance();
		} finally {
			Enhancer.registerCallbacks(factory, null);
		}
		return proxy;
	}

	/**
	 * Gets the proxy factory.
	 * 
	 * @param persistentClass
	 *            the persistent class
	 * @param interfaces
	 *            the interfaces
	 * @return the proxy factory
	 * @throws PersistenceException
	 *             the persistence exception
	 */
	public static Class getProxyFactory(Class persistentClass,
			Class[] interfaces) throws PersistenceException {
		Enhancer e = new Enhancer();
		e.setSuperclass(interfaces.length == 1 ? persistentClass : null);
		e.setInterfaces(interfaces);
		e
				.setCallbackTypes(new Class[] { InvocationHandler.class,
						NoOp.class, });
		e.setCallbackFilter(FINALIZE_FILTER);
		e.setUseFactory(false);
		e.setInterceptDuringConstruction(false);
		return e.createClass();
	}

	/**
	 * Instantiates a new cglib lazy initializer.
	 * 
	 * @param entityName
	 *            the entity name
	 * @param persistentClass
	 *            the persistent class
	 * @param interfaces
	 *            the interfaces
	 * @param id
	 *            the id
	 * @param getIdentifierMethod
	 *            the get identifier method
	 * @param setIdentifierMethod
	 *            the set identifier method
	 * @param em
	 *            the em
	 */
	private CglibLazyInitializer(final String entityName,
			final Class<?> persistentClass, final Class<?>[] interfaces,
			final String id, final Method getIdentifierMethod,
			final Method setIdentifierMethod, final EntityManagerImpl em) {

		this.entityName = entityName;
		this.id = id;
		this.em = em;
		this.persistentClass = persistentClass;
		this.getIdentifierMethod = getIdentifierMethod;
		this.setIdentifierMethod = setIdentifierMethod;
		this.interfaces = interfaces;
	}

	/*
	 * @see net.sf.cglib.proxy.InvocationHandler#invoke(java.lang.Object,
	 * java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		if (constructed) {

			String methodName = method.getName();
			int params = args.length;

			if (params == 0) {

				if (isUninitialized() && method.equals(getIdentifierMethod)) {
					return getIdentifier();
				}

				else if ("getKunderaLazyInitializer".equals(methodName)) {
					return this;
				}

			}

			Object target = getImplementation();
			try {
				final Object returnValue;
				if (method.isAccessible()) {
					if (!method.getDeclaringClass().isInstance(target)) {
						throw new ClassCastException(target.getClass()
								.getName());
					}
					returnValue = method.invoke(target, args);
				} else {
					if (!method.isAccessible()) {
						method.setAccessible(true);
					}
					returnValue = method.invoke(target, args);
				}
				return returnValue == target ? proxy : returnValue;
			} catch (InvocationTargetException ite) {
				throw ite.getTargetException();
			}
		} else {
			// while constructor is running
			throw new LazyInitializationException(
					"unexpected case hit, method=" + method.getName());
		}

	}

	/* @see com.impetus.kundera.proxy.LazyInitializer#getPersistentClass() */
	public final Class<?> getPersistentClass() {
		return persistentClass;
	}

	/**
	 * Gets the entity name.
	 * 
	 * @return the entity name {@inheritDoc}
	 */
	public final String getEntityName() {
		return entityName;
	}

	/**
	 * Gets the identifier.
	 * 
	 * @return the identifier {@inheritDoc}
	 */
	public final String getIdentifier() {
		return id;
	}

	/**
	 * Sets the identifier.
	 * 
	 * @param id
	 *            the new identifier {@inheritDoc}
	 */
	public final void setIdentifier(String id) {
		this.id = id;
	}

	/**
	 * Checks if is uninitialized.
	 * 
	 * @return true, if is uninitialized {@inheritDoc}
	 */
	public final boolean isUninitialized() {
		return !initialized;
	}

	/**
	 * Gets the entity manager.
	 * 
	 * @return the entity manager {@inheritDoc}
	 */
	public final EntityManagerImpl getEntityManager() {
		return em;
	}

	/**
	 * Unset entity manager.
	 * 
	 * {@inheritDoc}
	 */
	public void unsetEntityManager() {
		em = null;
	}

	/**
	 * Initialize.
	 * 
	 * @throws PersistenceException
	 *             the persistence exception {@inheritDoc}
	 */
	public final void initialize() throws PersistenceException {
		if (!initialized) {
			if (em == null) {
				throw new LazyInitializationException(
						"could not initialize proxy "
								+ persistentClass.getName() + "_" + id
								+ " - no EntityManager");
			} else if (!em.isOpen()) {
				throw new LazyInitializationException(
						"could not initialize proxy "
								+ persistentClass.getName() + "_" + id
								+ " - the owning Session was closed");
			} else {
				log.debug("Proxy >> Initialization >> "
						+ persistentClass.getName() + "_" + id);

				// TODO: consider not calling em.find from here. Not sure 'why',
				// but something
				// doesn't feel right.
				target = em.find(persistentClass, id);
				initialized = true;
			}
		}
	}

	/**
	 * Return the underlying persistent object, initializing if necessary.
	 * 
	 * @return the implementation
	 */
	public final Object getImplementation() {
		initialize();
		return target;
	}

	/**
	 * Getter for property 'target'.
	 * <p/>
	 * Same as {@link #getImplementation()} except that this method will not
	 * force initialization.
	 * 
	 * @return Value for property 'target'.
	 */
	protected final Object getTarget() {
		return target;
	}

	/**
	 * Checks if is unwrap.
	 * 
	 * @return true, if is unwrap {@inheritDoc}
	 */
	public boolean isUnwrap() {
		return unwrap;
	}

	/**
	 * Sets the unwrap.
	 * 
	 * @param unwrap
	 *            the new unwrap {@inheritDoc}
	 */
	public void setUnwrap(boolean unwrap) {
		this.unwrap = unwrap;
	}

}
