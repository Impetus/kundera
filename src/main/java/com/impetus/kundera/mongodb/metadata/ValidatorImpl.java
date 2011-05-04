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
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.api.Document;
import com.impetus.kundera.api.ColumnFamily;
import com.impetus.kundera.api.SuperColumnFamily;

/**
 * The Class CassandraEntityValidator.
 * 
 * @author animesh.kumar
 */
public class ValidatorImpl implements Validator {

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(ValidatorImpl.class);

    /** cache for validated classes. */
    private List<Class<?>> classes = new ArrayList<Class<?>>();

    /**
     * Checks the validity of a class for Cassandra entity.
     * 
     * @param clazz
     *            validates this class
     * 
     * @return returns 'true' if valid
     */
    @Override
    // TODO: reduce Cyclomatic complexity
    public final void validate(final Class<?> clazz) {

        if (classes.contains(clazz)) {
            return;
        }

        LOG.debug("Validating " + clazz.getName());

        // Is Entity?
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new PersistenceException(clazz.getName() + " is not annotated with @Entity");
        }

        // must have a default no-argument constructor
        try {
            clazz.getConstructor();
        } catch (NoSuchMethodException nsme) {
            throw new PersistenceException(clazz.getName() + " must have a default no-argument constructor.");
        }

        // what type is it? ColumnFamily or SuperColumnFamily, Document or simply relational entity?
        if (clazz.isAnnotationPresent(SuperColumnFamily.class) || clazz.isAnnotationPresent(ColumnFamily.class)) {
        	LOG.debug("Entity is for NoSQL database: " + clazz.getName());        	
        } else if(!clazz.isAnnotationPresent(Document.class)){
        	LOG.debug("Entity is for document based database: " + clazz.getName());
        } else {
        	LOG.debug("Entity is for relational database table: " + clazz.getName());
        }

        // check for @Key and ensure that there is just 1 @Key field of String
        // type.
        List<Field> keys = new ArrayList<Field>();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                keys.add(field);
            }
        }

        if (keys.size() == 0) {
            throw new PersistenceException(clazz.getName() + " must have an @Id field.");
        } else if (keys.size() > 1) {
            throw new PersistenceException(clazz.getName() + " can only have 1 @Id field.");
        }

        if (!keys.get(0).getType().equals(String.class)) {
            throw new PersistenceException(clazz.getName() + " @Id must be of String type.");
        }

        // save in cache
        classes.add(clazz);
    }
}
