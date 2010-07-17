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

import java.lang.reflect.Field;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Temporal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.MetadataProcessor;

/**
 * @author animesh.kumar
 *
 */
public abstract class AbstractEntityFieldProcessor implements MetadataProcessor {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(ColumnFamilyProcessor.class);

    
    public final String getValidJPAColumn (Class<?> entity, Field f) {

    	String name = null;

		if (f.isAnnotationPresent(Column.class)) {
			Column c = f.getAnnotation(Column.class);
			if (!c.name().isEmpty()) {
				name = c.name();
			} else {
				name = f.getName();
			}
		} else if (f.isAnnotationPresent(Basic.class)) {
			name = f.getName();
		}
		
		if (f.isAnnotationPresent(Temporal.class)) {
			if (!f.getType().equals(Date.class)) {
				log.error("@Temporal must map to java.util.Date for @Entity(" + entity.getName() + "." + f.getName() + ")");
				return name;
			}
			if (null == name) {
				name = f.getName();
			}
		}
		return name;
    }

}
