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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class ClassFileIterator.
 * 
 * @author animesh.kumar
 */
public class ClassFileIterator implements ResourceIterator {

    /** The ignored packages. */
    protected transient String[] ignoredPackages = { "javax", "java", "sun", "com.sun", "javassist" };

    /** The files. */
    private ArrayList<File> files;

    /** The index. */
    private int index = 0;

    /**
     * Instantiates a new class file iterator.
     * 
     * @param file
     *            the file
     * @param filter
     *            the filter
     */
    public ClassFileIterator(File file, Filter filter) {
        files = new ArrayList<File>();
        try {
            create(files, file, filter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the.
     * 
     * @param list
     *            the list
     * @param dir
     *            the dir
     * @param filter
     *            the filter
     * 
     * @throws Exception
     *             the exception
     */
    private static void create(List<File> list, File dir, Filter filter) throws Exception {
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                create(list, files[i], filter);
            } else {
                if (filter == null || filter.accepts(files[i].getAbsolutePath())) {
                    list.add(files[i]);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.ResourceIterator#next()
     */
    public InputStream next() {
        if (index >= files.size())
            return null;
        File fp = (File) files.get(index++);
        try {
            return new FileInputStream(fp);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.ResourceIterator#close()
     */
    public void close() {
    }
}
