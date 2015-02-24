/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.validation.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityAnnotationRule extends AbstractEntityRule implements EntityRule {

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(EntityAnnotationRule.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.validation.rules.AbstractEntityRule#validate(java .lang.Class)
     */
    @Override
    public void validate(Class<?> clazz) {

        if (log.isDebugEnabled())
            log.debug("Validating " + clazz.getName());

        // Is Entity?
        if (!checkValidClass(clazz)) {

            throw new RuleValidationException(clazz.getName() + " is not a valid jpa entity.");
        }

        // must have a default no-argument constructor
        boolean flag = false;
        try {
            Constructor[] constructors = clazz.getDeclaredConstructors();
            for (Constructor constructor : constructors) {
                if ((Modifier.isPublic(constructor.getModifiers()) || Modifier.isProtected(constructor.getModifiers()))
                    && constructor.getParameterTypes().length == 0) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                throw new Exception();
            }
        } catch (Exception e) {
            throw new RuleValidationException(clazz.getName()
                + " must have a default public or protected no-argument constructor.");
        }
    }

    /**
     * checks for a valid entity definition
     * 
     * @param clazz
     * @return
     */
    private boolean checkValidClass(Class<?> clazz) {
        return clazz.isAnnotationPresent(Entity.class) || clazz.isAnnotationPresent(MappedSuperclass.class)
            || clazz.isAnnotationPresent(Embeddable.class);

    }

}