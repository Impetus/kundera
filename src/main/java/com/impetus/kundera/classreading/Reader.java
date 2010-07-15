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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.annotation.Annotation;

/**
 * Base class-reader.
 * 
 * @author animesh.kumar
 */
public abstract class Reader {

    /** The valid annotations. */
    private List<String> validAnnotations = new ArrayList<String>();

    /** The annotation discovery listeners. */
    private List<AnnotationDiscoveryListener> annotationDiscoveryListeners = new ArrayList<AnnotationDiscoveryListener>();

    /**
     * Instantiates a new reader.
     */
    public Reader() {
    }

    /**
     * Scan class.
     * 
     * @param bits
     *            the bits
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void scanClass(InputStream bits) throws IOException {
        DataInputStream dstream = new DataInputStream(new BufferedInputStream(bits));
        ClassFile cf = null;
        try {
            cf = new ClassFile(dstream);

            String className = cf.getName();
            List<String> annotations = new ArrayList<String>();

            accumulateAnnotations(annotations, (AnnotationsAttribute) cf.getAttribute(AnnotationsAttribute.visibleTag));
            accumulateAnnotations(annotations, (AnnotationsAttribute) cf.getAttribute(AnnotationsAttribute.invisibleTag));

            // iterate through all valid annotations
            for (String validAnn : getValidAnnotations()) {
                // check if the current class has one?
                if (annotations.contains(validAnn)) {
                    // fire all listeners
                    for (AnnotationDiscoveryListener listener : getAnnotationDiscoveryListeners()) {
                        listener.discovered(className, annotations.toArray(new String[] {}));
                    }
                }
            }

        } finally {
            dstream.close();
            bits.close();
        }
    }

    // helper method to accumulate annotations.
    /**
     * Accumulate annotations.
     * 
     * @param annotations
     *            the annotations
     * @param annatt
     *            the annatt
     */
    private void accumulateAnnotations(List<String> annotations, AnnotationsAttribute annatt) {
        if (null == annatt)
            return;
        for (Annotation ann : annatt.getAnnotations()) {
            annotations.add(ann.getTypeName());
        }
    }

    /**
     * Gets the resource iterator.
     * 
     * @param url
     *            the url
     * @param filter
     *            the filter
     * 
     * @return the resource iterator
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public ResourceIterator getResourceIterator(URL url, Filter filter) throws IOException {
        String urlString = url.toString();
        if (urlString.endsWith("!/")) {
            urlString = urlString.substring(4);
            urlString = urlString.substring(0, urlString.length() - 2);
            url = new URL(urlString);
        }

        if (!urlString.endsWith("/")) {
            return new JarFileIterator(url.openStream(), filter);
        } else {

            if (!url.getProtocol().equals("file")) {
                throw new IOException("Unable to understand protocol: " + url.getProtocol());
            }

            File f = new File(url.getPath());
            if (f.isDirectory()) {
                return new ClassFileIterator(f, filter);
            } else {
                return new JarFileIterator(url.openStream(), filter);
            }
        }
    }

    /**
     * Gets the valid annotations.
     * 
     * @return the valid annotations
     */
    public List<String> getValidAnnotations() {
        return validAnnotations;
    }

    /**
     * Adds the valid annotations.
     * 
     * @param annotation
     *            the annotation
     */
    public void addValidAnnotations(String annotation) {
        this.validAnnotations.add(annotation);
    }

    /**
     * Gets the annotation discovery listeners.
     * 
     * @return the annotation discovery listeners
     */
    public List<AnnotationDiscoveryListener> getAnnotationDiscoveryListeners() {
        return annotationDiscoveryListeners;
    }

    /**
     * Adds the annotation discovery listeners.
     * 
     * @param annotationDiscoveryListener
     *            the annotation discovery listener
     */
    public void addAnnotationDiscoveryListeners(AnnotationDiscoveryListener annotationDiscoveryListener) {
        this.annotationDiscoveryListeners.add(annotationDiscoveryListener);
    }

    /**
     * Read.
     */
    public abstract void read();

    /**
     * Gets the filter.
     * 
     * @return the filter
     */
    public abstract Filter getFilter();

}
