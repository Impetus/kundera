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
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.loader.PersistenceXMLLoader.AllowedProtocol;

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
     * Scan class resource in the provided urls with the additional Class-Path
     * of each jar checking
     * 
     * @param classRelativePath
     *            relative path to a class resource
     * @param urls
     *            urls to be checked
     * @return list of class path included in the base package
     */
    private URL[] findResourcesInUrls(String classRelativePath, URL[] urls)
    {
        List<URL> list = new ArrayList<URL>();
        for (URL url : urls)
        {
            if (AllowedProtocol.isValidProtocol(url.getProtocol().toUpperCase()) && url.getPath().endsWith(".jar"))
            {
                try
                {
                    JarFile jarFile = new JarFile(URLDecoder.decode(url.getFile(), Constants.CHARSET_UTF8));

                    // Checking the dependencies of this jar file
                    Manifest manifest = jarFile.getManifest();
                    if (manifest != null)
                    {
                        String classPath = manifest.getMainAttributes().getValue("Class-Path");
                        // Scan all entries in the classpath if they are
                        // specified in the jar
                        if (!StringUtils.isEmpty(classPath))
                        {
                            List<URL> subList = new ArrayList<URL>();
                            for (String cpEntry : classPath.split(" "))
                            {
                                try
                                {
                                    subList.add(new URL(cpEntry));
                                }
                                catch (MalformedURLException e)
                                {
                                    URL subResources = ClasspathReader.class.getClassLoader().getResource(cpEntry);
                                    if (subResources != null)
                                    {
                                        subList.add(subResources);
                                    }
                                    // logger.warn("Incorrect URL in the classpath of a jar file ["
                                    // + url.toString()
                                    // + "]: " + cpEntry);
                                }
                            }
                            list.addAll(Arrays.asList(findResourcesInUrls(classRelativePath,
                                    subList.toArray(new URL[subList.size()]))));
                        }
                    }
                    JarEntry present = jarFile.getJarEntry(classRelativePath + ".class");
                    if (present != null)
                    {
                        list.add(url);
                    }
                }
                catch (IOException e)
                {
                    logger.warn("Error during loading from context , Caused by:" + e.getMessage());
                }

            }
            else if (url.getPath().endsWith("/"))
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
        return list.toArray(new URL[list.size()]);
    }

    /**
     * Scan class resources into a basePackagetoScan path.
     * 
     * @return list of class path included in the base package
     */

    private final URL[] findResourcesByContextLoader()
    {
        List<URL> list = new ArrayList<URL>();
        ClassLoader classLoader = this.getClass().getClassLoader();
        assert classLoader != null;        
        URL[] urls = ((URLClassLoader) classLoader).getURLs();
        
        for (String fullyQualifiedClassName : classesToScan)
        {
            String classRelativePath = fullyQualifiedClassName.replace(".", "/");
            list.addAll(Arrays.asList(findResourcesInUrls(classRelativePath, urls)));
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
        // else
        // {
        // result = findResourcesByClasspath();
        // }
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
    
    @Override
    public InputStream[] findResourcesAsStream()
    {
        ClassLoader classLoader = this.getClass().getClassLoader();
        assert classLoader != null;

        InputStream[] iStreams = null;
        int counter = 0;
        if(classesToScan != null)
        {
            for (String fullyQualifiedClassName : classesToScan)
            {
                if (iStreams == null)
                {
                    iStreams = new InputStream[classesToScan.size()];
                }

                String classRelativePath = fullyQualifiedClassName.replace(".", "/");
                InputStream is = this.getClass().getClassLoader().getResourceAsStream(classRelativePath + ".class");
                iStreams[counter++] = is;
            }
        }
        return iStreams;

    }
}