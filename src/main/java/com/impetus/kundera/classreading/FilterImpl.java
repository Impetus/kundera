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
package com.impetus.kundera.classreading;

/**
 * Basic implementation to skip well-known packages and allow only *.class files
 * 
 * @author animesh.kumar
 */
public class FilterImpl implements Filter {

    /** The ignored packages. */
    private transient String[] ignoredPackages = { "javax", "java", "sun", "com.sun", "javassist" };

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.Filter#accepts(java.lang.String)
     */
    @Override
    public final boolean accepts(String filename) {
        if (filename.endsWith(".class")) {
            if (filename.startsWith("/")) {
                filename = filename.substring(1);
            }
            if (!ignoreScan(filename.replace('/', '.'))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ignore scan.
     * 
     * @param intf
     *            the intf
     * 
     * @return true, if successful
     */
    private boolean ignoreScan(String intf) {
        for (String ignored : ignoredPackages) {
            if (intf.startsWith(ignored + ".")) {
                return true;
            }
        }
        return false;
    }

}
