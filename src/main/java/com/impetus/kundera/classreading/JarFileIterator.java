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
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Iterates through a Jar file for each file resource.
 * 
 * @author animesh.kumar
 */
public final class JarFileIterator implements ResourceIterator {

    /** The jar. */
    private JarInputStream jar;

    /** The next. */
    private JarEntry next;

    /** The filter. */
    private Filter filter;

    /** The initial. */
    private boolean initial = true;

    /** The closed. */
    private boolean closed = false;

    /**
     * Instantiates a new jar file iterator.
     * 
     * @param file
     *            the file
     * @param filter
     *            the filter
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public JarFileIterator(File file, Filter filter) throws IOException {
        this(new FileInputStream(file), filter);
    }

    /**
     * Instantiates a new jar file iterator.
     * 
     * @param is
     *            the is
     * @param filter
     *            the filter
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public JarFileIterator(InputStream is, Filter filter) throws IOException {
        this.filter = filter;
        jar = new JarInputStream(is);
    }

    /**
     * Sets the next.
     */
    private void setNext() {
        initial = true;
        try {
            if (next != null) {
                jar.closeEntry();
            }
            next = null;

            do {
                next = jar.getNextJarEntry();
            } while (next != null && (next.isDirectory() || (filter == null || !filter.accepts(next.getName()))));

            if (next == null) {
                close();
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to browse jar", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.ResourceIterator#next()
     */
    public InputStream next() {
        if (closed || (next == null && !initial))
            return null;
        setNext();
        if (next == null)
            return null;
        return new InputStreamWrapper(jar);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.ResourceIterator#close()
     */
    public void close() {
        try {
            closed = true;
            jar.close();
        } catch (IOException ignored) {

        }

    }

    /**
     * The Class InputStreamWrapper.
     */
    class InputStreamWrapper extends InputStream {

        /** The delegate. */
        private InputStream delegate;

        /**
         * Instantiates a new input stream wrapper.
         * 
         * @param delegate
         *            the delegate
         */
        public InputStreamWrapper(InputStream delegate) {
            this.delegate = delegate;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#read()
         */
        public int read() throws IOException {
            return delegate.read();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#read(byte[])
         */
        public int read(byte[] bytes) throws IOException {
            return delegate.read(bytes);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#read(byte[], int, int)
         */
        public int read(byte[] bytes, int i, int i1) throws IOException {
            return delegate.read(bytes, i, i1);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#skip(long)
         */
        public long skip(long l) throws IOException {
            return delegate.skip(l);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#available()
         */
        public int available() throws IOException {
            return delegate.available();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#close()
         */
        public void close() throws IOException {
            // ignored
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#mark(int)
         */
        public void mark(int i) {
            delegate.mark(i);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#reset()
         */
        public void reset() throws IOException {
            delegate.reset();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.io.InputStream#markSupported()
         */
        public boolean markSupported() {
            return delegate.markSupported();
        }
    }
}
