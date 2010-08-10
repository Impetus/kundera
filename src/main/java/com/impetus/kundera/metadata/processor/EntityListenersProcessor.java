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
package com.impetus.kundera.metadata.processor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityListeners;
import javax.persistence.PersistenceException;
import javax.persistence.PostLoad;
import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.event.CallbackMethod;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.MetadataProcessor;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The MetadataProcessor implementation to scan for EntityListener class/method
 * JPA Specifications: 1. EntityListeners classes must have a no-argument
 * constructor. 2. Callback methods can have any visibility. 3. Callback methods
 * must return void. 4. Callback methods must NOT throw any checked exception.
 * 5. ExternalCallback methods must accept only entity object. 6.
 * InternalCallback methods must NOT accept any parameter. 7. EntityListeners
 * are state-less. 8. EnternalCallbackMethods must be fired before
 * InternalCallbackMethods.
 * 
 * @author animesh.kumar
 */

public class EntityListenersProcessor implements MetadataProcessor {

	/** the log used by this class. */
	private static Log log = LogFactory.getLog(EntityListenersProcessor.class);

	// list of all valid JPA Entity Listeners
	/** The Constant JPAListenersAnnotations. */
	@SuppressWarnings("unchecked")
	private static final List<Class<? extends Annotation>> JPAListenersAnnotations = Arrays
			.asList(PrePersist.class, PostPersist.class, PreUpdate.class,
					PostUpdate.class, PreRemove.class, PostRemove.class,
					PostLoad.class);

	/*
	 * @see
	 * com.impetus.kundera.metadata.MetadataProcessor#process(java.lang.Class,
	 * com.impetus.kundera.metadata.EntityMetadata)
	 */
	@Override
	public final void process(final Class<?> entityClass,
			EntityMetadata metadata) {

		// list all external listeners first.
		EntityListeners entityListeners = (EntityListeners) entityClass
				.getAnnotation(EntityListeners.class);
		if (entityListeners != null) {
			Class<?>[] entityListenerClasses = entityListeners.value();
			if (entityListenerClasses != null) {
				// iterate through all EntityListeners
				for (Class<?> entityListener : entityListenerClasses) {

					// entityListener class must have a no-argument constructor
					try {
						entityListener.getConstructor();
					} catch (NoSuchMethodException nsme) {
						throw new PersistenceException(
								"Skipped method("
										+ entityListener.getName()
										+ ") must have a default no-argument constructor.");
					}

					// iterate through all public methods
					for (Method method : entityListener.getDeclaredMethods()) {

						// find valid jpa annotations for this method
						List<Class<?>> jpaAnnotations = getValidJPAAnnotationsFromMethod(
								entityListener, method, 1);

						// add them all to metadata
						for (Class<?> jpaAnnotation : jpaAnnotations) {
							CallbackMethod callBackMethod = metadata.new ExternalCallbackMethod(
									entityListener, method);
							addCallBackMethod(metadata, jpaAnnotation,
									callBackMethod);
						}
					}
				}
			}
		}

		// list all internal listeners now.
		// iterate through all public methods of entityClass
		// since this is already an @Entity class, it will sure have a default
		// no-arg constructor
		for (Method method : entityClass.getDeclaredMethods()) {
			// find valid jpa annotations for this method
			List<Class<?>> jpaAnnotations = getValidJPAAnnotationsFromMethod(
					entityClass, method, 0);
			// add them all to metadata
			for (Class<?> jpaAnnotation : jpaAnnotations) {
				CallbackMethod callbackMethod = metadata.new InternalCallbackMethod(
						method);
				addCallBackMethod(metadata, jpaAnnotation, callbackMethod);
			}
		}
	}

	/**
	 * Adds the call back method.
	 * 
	 * @param metadata
	 *            the metadata
	 * @param jpaAnnotation
	 *            the jpa annotation
	 * @param callbackMethod
	 *            the callback method
	 */
	@SuppressWarnings("unchecked")
	private void addCallBackMethod(EntityMetadata metadata,
			Class<?> jpaAnnotation, CallbackMethod callbackMethod) {
		Map<Class<?>, List<? extends CallbackMethod>> callBackMethodsMap = metadata
				.getCallbackMethodsMap();
		List<CallbackMethod> list = (List<CallbackMethod>) callBackMethodsMap
				.get(jpaAnnotation);
		if (null == list) {
			list = new ArrayList<CallbackMethod>();
			callBackMethodsMap.put(jpaAnnotation, list);
		}
		list.add(callbackMethod);
	}

	/**
	 * Gets the valid jpa annotations from method.
	 * 
	 * @param clazz
	 *            the clazz
	 * @param method
	 *            the method
	 * @param numberOfParams
	 *            the number of params
	 * 
	 * @return the valid jpa annotations from method
	 */
	private List<Class<?>> getValidJPAAnnotationsFromMethod(Class<?> clazz,
			Method method, int numberOfParams) {
		List<Class<?>> annotations = new ArrayList<Class<?>>();

		for (Annotation methodAnnotation : method.getAnnotations()) {
			Class<?> methodAnnotationType = methodAnnotation.annotationType();

			if (isValidJPAEntityListenerAnnotation(methodAnnotationType)) {

				// verify method signature

				// verify exceptions
				boolean hasUncheckedExceptions = false;
				for (Class<?> exception : method.getExceptionTypes()) {
					if (!ReflectUtils.hasSuperClass(RuntimeException.class,
							exception)) {
						hasUncheckedExceptions = true;
						break;
					}
				}

				if (hasUncheckedExceptions) {
					log.info("Skipped method(" + clazz.getName() + "."
							+ method.getName()
							+ ") Must not throw unchecked exceptions.");
					continue;
				}

				// return type must be "void"
				if (!method.getReturnType().getSimpleName().equals("void")) {
					log.info("Skipped method(" + clazz.getName() + "."
							+ method.getName()
							+ ") Must have \"void\" return type.");
					continue;
				}
				// argument must be an Entity or Object
				Class<?>[] paramTypes = method.getParameterTypes();
				if (paramTypes.length != numberOfParams) {
					log.info("Skipped method(" + clazz.getName() + "."
							+ method.getName() + ") Must have "
							+ numberOfParams + " parameter.");
					continue;
				}

				if (numberOfParams == 1) {
					Class<?> parameter = paramTypes[0];
					if (!parameter.getName().equals("java.lang.Object")) {
						log
								.info("Skipped method("
										+ clazz.getName()
										+ "."
										+ method.getName()
										+ ") Must have only 1 \"Object\" type parameter.");
						continue;
					}
				}

				annotations.add(methodAnnotationType);
			}
		}
		return annotations;
	}

	/**
	 * Checks if is valid jpa entity listener annotation.
	 * 
	 * @param annotation
	 *            the annotation
	 * 
	 * @return true, if is valid jpa entity listener annotation
	 */
	private boolean isValidJPAEntityListenerAnnotation(Class<?> annotation) {
		return JPAListenersAnnotations.contains(annotation);
	}

}
