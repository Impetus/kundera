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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ClasspathReader.
 * 
 * @author animesh.kumar
 */
public class ClasspathReader extends Reader
{
    private static Logger logger = LoggerFactory.getLogger(ClasspathReader.class);

    /**
     * The filter.
     */
    private Filter filter;

    /** The classes to scan. */
    private List<String> classesToScan;

    /**
     * Instantiates a new classpath reader.
     */
    public ClasspathReader()
    {
        filter = new FilterImpl();
    }

    /**
     * Instantiates a new classpath reader.
     * 
     * @param classesToScan
     *            the classes to scan
     */
    public ClasspathReader(List<String> classesToScan)
    {
        this();
        this.classesToScan = classesToScan;
    }

    @Override
    public final void read()
    {
        URL[] resources = findResources();
        for (URL resource : resources)
        {

            try
            {
                ResourceIterator itr = getResourceIterator(resource, getFilter());

                InputStream is = null;
                while ((is = itr.next()) != null)
                {
                    scanClass(is);
                }
            }
            catch (IOException e)
            {
                logger.error("Error during reading via classpath, Caused by:" + e.getMessage());
                throw new ResourceReadingException(e);
            }
        }
    }

    /**
     * Uses the java.class.path system property to obtain a list of URLs that
     * represent the CLASSPATH
     * 
     * @return the URl[]
     */
    @SuppressWarnings("deprecation")
    @Override
    public final URL[] findResourcesByClasspath()
    {
        List<URL> list = new ArrayList<URL>();
        String classpath = System.getProperty("java.class.path");
        StringTokenizer tokenizer = new StringTokenizer(classpath, File.pathSeparator);

        while (tokenizer.hasMoreTokens())
        {
            String path = tokenizer.nextToken();

            File fp = new File(path);
            if (!fp.exists())
                throw new ResourceReadingException("File in java.class.path does not exist: " + fp);
            try
            {
                list.add(fp.toURL());
            }
            catch (MalformedURLException e)
            {
                throw new ResourceReadingException(e);
            }
        }
        return list.toArray(new URL[list.size()]);
    }

    /**
     * Scan class resources into a basePackagetoScan path.
     * 
     * @return list of class path included in the base package
     */

    @Override
    public final URL[] findResourcesByContextLoader()
    {
        List<URL> list = new ArrayList<URL>();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;

        for (String fullyQualifiedClassName : classesToScan)
        {
            String classRelativePath = fullyQualifiedClassName.replace(".", "/");

            URL[] urls = ((URLClassLoader) classLoader).getURLs();

            for (URL url : urls)
            {
                if (url.getPath().endsWith("/"))
                {
                    File file = new File(url.getPath() + classRelativePath + ".class");
                    if (file.exists())
                    {
                        try
                        {
                            list.add(file.toURL());
                        }
                        catch (MalformedURLException e)
                        {
                            throw new ResourceReadingException(e);
                        }
                    }
                }

            }

        }

        return list.toArray(new URL[list.size()]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.Reader#findResources()
     */
    @Override
    public URL[] findResources()
    {
        URL[] result = null;

        if (classesToScan != null && !classesToScan.isEmpty())
        {
            result = findResourcesByContextLoader();
        }
        else
        {
            result = findResourcesByClasspath();
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.classreading.Reader#getFilter()
     */

    public final Filter getFilter()
    {
        return filter;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public final void setFilter(Filter filter)
    {
        this.filter = filter;
    }
}
