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

import com.impetus.kundera.classreading.AnnotationDiscoveryListener;

/**
 * An interface to act as a listener for AnnotationDiscovery.
 * 
 * @author animesh.kumar
 * @see com.impetus.kundera.metadata.MetadataManager
 */
public class EntityAnnotationDiscoveryListener implements AnnotationDiscoveryListener {

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.classreading.AnnotationDiscoveryListener#discovered
     * (java.lang.String, java.lang.String[])
     */
    @Override
    // called whenever a class with the supplied annotation is encountered in
    // the classpath.
    public void discovered(String className, String[] annotations) {
        System.out.println(className);
    }
}
