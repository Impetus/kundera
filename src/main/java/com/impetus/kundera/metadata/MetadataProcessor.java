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

import javax.persistence.PersistenceException;

/**
 * The Interface IMetadataProcessor.
 * 
 * @author animesh.kumar
 */
public interface MetadataProcessor {

    /**
     * Process.
     * 
     * @param clazz	the clazz
     * @param metadata  the metadata
     * 
     * @throws PersistenceException  the illegal entity exception
     */
    void process(Class<?> clazz, EntityMetadata metadata) throws PersistenceException;
}
