/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.classreading;

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Iterates through a Jar file for each file resource.
 * 
 * @author animesh.kumar
 */
public final class JarFileIterator implements ResourceIterator
{

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
    /*
     * public JarFileIterator(File file, Filter filter) throws
     * FileNotFoundException { this(new FileInputStream(file), filter); }
     */

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
    public JarFileIterator(InputStream is, Filter filter)
    {
        this.filter = filter;
        try
        {
            jar = new JarInputStream(is);
        }
        catch (IOException e)
        {
            throw new ResourceReadingException(e);
        }
    }

    /**
     * Sets the next.
     */
    private void setNext()
    {
        initial = true;
        try
        {
            if (next != null)
            {
                jar.closeEntry();
            }
            next = null;

            do
            {
                next = jar.getNextJarEntry();
            }
            while (next != null && (next.isDirectory() || (filter == null || !filter.accepts(next.getName()))));

            if (next == null)
            {
                close();
            }
        }
        catch (IOException e)
        {
            throw new ResourceReadingException("Failed to browse jar:", e);
        }
    }

    public InputStream next()
    {
        if (closed || (next == null && !initial))
            return null;

        setNext();

        if (next == null)
            return null;
        return new InputStreamWrapper(jar);
    }

    public void close()
    {
        try
        {
            closed = true;
            jar.close();
        }
        catch (IOException ignored)
        {

        }

    }

    /**
     * The Class InputStreamWrapper.
     */
    class InputStreamWrapper extends InputStream
    {

        /** The delegate. */
        private InputStream delegate;

        /**
         * Instantiates a new input stream wrapper.
         * 
         * @param delegate
         *            the delegate
         */
        public InputStreamWrapper(InputStream delegate)
        {
            this.delegate = delegate;
        }

        public int read() throws IOException
        {
            return delegate.read();
        }

        public int read(byte[] bytes) throws IOException
        {
            return delegate.read(bytes);
        }

        public int read(byte[] bytes, int i, int i1) throws IOException
        {
            return delegate.read(bytes, i, i1);
        }

        public long skip(long l) throws IOException
        {
            return delegate.skip(l);
        }

        public int available() throws IOException
        {
            return delegate.available();
        }

        public void close() throws IOException
        {
            // ignored
        }

        public void mark(int i)
        {
            delegate.mark(i);
        }

        public void reset() throws IOException
        {
            try
            {
                delegate.reset();
            }
            catch (IOException e)
            {
                throw e;
            }
        }

        public boolean markSupported()
        {
            return delegate.markSupported();
        }
    }
}
