/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.metadata.model;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.loader.PersistenceLoaderException;

/**
 * The Class PersistenceUnitMetadata.
 * 
 * @author amresh.singh
 */
public class PersistenceUnitMetadata implements PersistenceUnitInfo
{
    /** logger instance. */
    private static Logger log = LoggerFactory.getLogger(PersistenceUnitMetadata.class);

    /** Persistence Unit name. */
    private String persistenceUnitName;

    /** The provider. */
    private String provider;

    /** The transaction type. */
    private PersistenceUnitTransactionType transactionType;

    /** The classes. */
    private List<String> classes = new ArrayList<String>();

    /** The packages. */
    private List<String> packages = new ArrayList<String>();

    /** The jar files. */
    private Set<String> jarFiles;

    private Set<URL> jarUrls;

    /** The properties. */
    private Properties properties = new Properties();

    /** The exclude unlisted classes. */
    private boolean excludeUnlistedClasses = false;

    private URL rootUrl;

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getPersistenceUnitName()
     */
    @Override
    public String getPersistenceUnitName()
    {
        return persistenceUnitName;
    }

    /**
     * Sets the persistence unit name.
     * 
     * @param persistenceUnitName
     *            the persistenceUnitName to set
     */
    public void setPersistenceUnitName(String persistenceUnitName)
    {
        this.persistenceUnitName = persistenceUnitName;
    }

    /**
     * Sets the transaction type.
     * 
     * @param transactionType
     *            the new transaction type
     */
    public void setTransactionType(PersistenceUnitTransactionType transactionType)
    {
        this.transactionType = transactionType;
    }

    /**
     * Gets the provider.
     * 
     * @return the provider
     */
    public String getProvider()
    {
        return provider;
    }

    /**
     * Sets the provider.
     * 
     * @param provider
     *            the new provider
     */
    public void setProvider(String provider)
    {
        if (provider != null && provider.endsWith(".class"))
        {
            this.provider = provider.substring(0, provider.length() - 6);
        }
        this.provider = provider;
    }

    /**
     * Gets the classes.
     * 
     * @return the classes
     */
    public List<String> getClasses()
    {
        return classes;
    }

    /**
     * Sets the classes.
     * 
     * @param classes
     *            the new classes
     */
    public void setClasses(List<String> classes)
    {
        this.classes = classes;
    }

    /**
     * Gets the packages.
     * 
     * @return the packages
     */
    public List<String> getPackages()
    {
        return packages;
    }

    /**
     * Sets the packages.
     * 
     * @param packages
     *            the new packages
     */
    public void setPackages(List<String> packages)
    {
        this.packages = packages;
    }

    /**
     * Gets the jar files.
     * 
     * @return the jar files
     */
    public Set<String> getJarFiles()
    {
        return jarFiles;
    }

    /**
     * Sets the jar files.
     * 
     * @param jarFiles
     *            the new jar files
     */
    public void addJarFile(String jarFile)
    {
        if (jarFiles == null)
        {
            jarFiles = new HashSet<String>();
        }
        this.jarFiles.add(jarFile);
        addJarFileUrl(jarFile);

    }

    /**
     * Gets the exclude unlisted classes.
     * 
     * @return the exclude unlisted classes
     */
    public boolean getExcludeUnlistedClasses()
    {
        return excludeUnlistedClasses;
    }

    /**
     * Sets the exclude unlisted classes.
     * 
     * @param excludeUnlistedClasses
     *            the new exclude unlisted classes
     */
    public void setExcludeUnlistedClasses(boolean excludeUnlistedClasses)
    {
        this.excludeUnlistedClasses = excludeUnlistedClasses;
    }

    /* @see java.lang.Object#toString() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("PersistenceMetadata [name=");
        builder.append(persistenceUnitName);
        builder.append(", provider=");
        builder.append(provider);
        builder.append(", transactionType=");
        builder.append(transactionType);
        builder.append(", classes=");
        builder.append(classes);
        builder.append(", excludeUnlistedClasses=");
        builder.append(excludeUnlistedClasses);
        builder.append(", jarFiles=");
        builder.append(jarFiles);
        builder.append(", packages=");
        builder.append(packages);
        builder.append(", props=");
        builder.append(properties);
        builder.append("]");
        return builder.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.spi.PersistenceUnitInfo#getPersistenceProviderClassName
     * ()
     */
    @Override
    public String getPersistenceProviderClassName()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getTransactionType()
     */
    @Override
    public PersistenceUnitTransactionType getTransactionType()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getJtaDataSource()
     */
    @Override
    public DataSource getJtaDataSource()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getNonJtaDataSource()
     */
    @Override
    public DataSource getNonJtaDataSource()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getMappingFileNames()
     */
    @Override
    public List<String> getMappingFileNames()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getJarFileUrls()
     */
    @Override
    public List<URL> getJarFileUrls()
    {
        return jarUrls != null ? new ArrayList<URL>(jarUrls) : null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.spi.PersistenceUnitInfo#getPersistenceUnitRootUrl()
     */
    @Override
    public URL getPersistenceUnitRootUrl()
    {
        return rootUrl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getManagedClassNames()
     */
    @Override
    public List<String> getManagedClassNames()
    {
        return classes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#excludeUnlistedClasses()
     */
    @Override
    public boolean excludeUnlistedClasses()
    {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getSharedCacheMode()
     */
    @Override
    public SharedCacheMode getSharedCacheMode()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getValidationMode()
     */
    @Override
    public ValidationMode getValidationMode()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getProperties()
     */
    @Override
    public Properties getProperties()
    {
        if (this.properties == null)
        {
            throw new PersistenceLoaderException(" Error while loading metadata as perssitenceUnitMetadata is null");
        }
        return this.properties;
    }

    /**
     * Sets the properties.
     * 
     * @param properties
     *            the properties to set
     */
    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.spi.PersistenceUnitInfo#getPersistenceXMLSchemaVersion
     * ()
     */
    @Override
    public String getPersistenceXMLSchemaVersion()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getClassLoader()
     */
    @Override
    public ClassLoader getClassLoader()
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.spi.PersistenceUnitInfo#addTransformer(javax.persistence
     * .spi.ClassTransformer)
     */
    @Override
    public void addTransformer(ClassTransformer paramClassTransformer)
    {
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.spi.PersistenceUnitInfo#getNewTempClassLoader()
     */
    @Override
    public ClassLoader getNewTempClassLoader()
    {
        return null;
    }

    /**
     * Gets the property.
     * 
     * @param prop
     *            the prop
     * @return the property
     */
    public String getProperty(String prop)
    {
        // assuming Properties are initialized with this call
        return prop != null ? getProperties().getProperty(prop) : null;
    }

    /**
     * Returns list of managed urls.
     * 
     * @return
     */
    public List<URL> getManagedURLs()
    {
        // should we cache it?
        List<URL> managedURL = getJarFileUrls();
        if (managedURL == null)
        {
            managedURL = new ArrayList<URL>(1);
        }

        if (!getExcludeUnlistedClasses())
        {
            managedURL.add(getPersistenceUnitRootUrl());
        }
        return managedURL;
    }

    /**
     * Setter for persistence unit root url
     * 
     * @param url
     *            persistence unit root url.
     */
    public void setPersistenceUnitRootUrl(URL url)
    {
        this.rootUrl = url;
    }

    /**
     * Adds jar file URL.
     * 
     * @param jarFile
     *            jar file path
     */
    private void addJarFileUrl(String jarFile)
    {
        if (jarUrls == null)
        {
            jarUrls = new HashSet<URL>();
        }
        try
        {
            jarUrls.add(new File(jarFile).toURI().toURL());
        }
        catch (MalformedURLException e)
        {
            log.error("Error while mapping jar-file url" + jarFile + "caused by:" + e.getMessage());
            throw new IllegalArgumentException("Invalid jar-file URL:" + jarFile + "caused by: " + e.getMessage());
        }
    }

    /**
     * Gets the client. In case client is not configure, it throws
     * IllegalArgumentException.
     * 
     * @return the client
     */
    public String getClient()
    {
        String client = null;
        if (this.properties != null)
        {
            client = (String) this.properties.get(PersistenceProperties.KUNDERA_CLIENT_FACTORY);
        }

        if (client == null)
        {
            log.error("kundera.client property is missing for persistence unit:" + persistenceUnitName);
            throw new IllegalArgumentException("kundera.client property is missing for persistence unit:"
                    + persistenceUnitName);
        }

        return client;
    }
}
