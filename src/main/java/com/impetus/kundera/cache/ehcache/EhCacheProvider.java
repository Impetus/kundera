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
package com.impetus.kundera.cache.ehcache;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.event.CacheEventListener;
import net.sf.ehcache.util.ClassLoaderUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.cache.Cache;
import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.cache.CacheProvider;
import com.impetus.kundera.ejb.EntityManagerImpl;

/**
 * Cache provider implementation using Ehcache
 * 
 * @author animesh.kumar
 *
 */
public class EhCacheProvider implements CacheProvider {

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityManagerImpl.class);

    private CacheManager manager;
    private static final String NET_SF_EHCACHE_CONFIGURATION_RESOURCE_NAME = "net.sf.ehcache.configurationResourceName";
    private boolean initializing;
    private List<CacheEventListener> listeners = new ArrayList<CacheEventListener>();


    public synchronized void init(Map properties) throws CacheException {
        if (manager != null) {
            log.warn ("Attempt to restart an already started CacheFactory. Using previously created EhCacheFactory.");
            return;
        }
        initializing = true;
        try {
            String configurationResourceName = null;
            if (properties != null) {
                configurationResourceName = (String) properties.get(NET_SF_EHCACHE_CONFIGURATION_RESOURCE_NAME);
            }
            if (configurationResourceName == null || configurationResourceName.length() == 0) {
                manager = new CacheManager();
            } else {
                if (!configurationResourceName.startsWith("/")) {
                    configurationResourceName = "/" + configurationResourceName;
                    	log.info ("prepending / to " + configurationResourceName + ". It should be placed in the root"
                                + "of the classpath rather than in a package.");
                }
                URL url = loadResource(configurationResourceName);
                manager = new CacheManager(url);
            }
        } catch (net.sf.ehcache.CacheException e) {
            if (e.getMessage().startsWith("Cannot parseConfiguration CacheManager. Attempt to create a new instance of " +
                    "CacheManager using the diskStorePath")) {
                throw new CacheException("Could not init EhCacheFactory.", e);
            } else {
                throw e;
            }
        } finally {
            initializing = false;
        }

    }

    private URL loadResource(String configurationResourceName) {
        ClassLoader standardClassloader = ClassLoaderUtil.getStandardClassLoader();
        URL url = null;
        if (standardClassloader != null) {
            url = standardClassloader.getResource(configurationResourceName);
        }
        if (url == null) {
            url = this.getClass().getResource(configurationResourceName);
        }
            log.info ("Creating EhCacheFactory from a specified resource: "
                    + configurationResourceName + " Resolved to URL: " + url);

    	if (url == null) {
            log.warn ("A configurationResourceName was set to " + configurationResourceName +
                    " but the resource could not be loaded from the classpath." +
                    "Ehcache will configure itself using defaults.");
        }
        return url;
    }
    
    @Override
	public Cache createCache(String name) throws CacheException {
        if (manager == null) {
            throw new CacheException("CacheFactory was not initialized. Call init() before creating a cache.");
        }
        try {
            net.sf.ehcache.Cache cache = manager.getCache(name);
            if (cache == null) {
                log.warn ("Could not find a specific ehcache configuration for cache named [" + name + "]; using defaults.");
                manager.addCache(name);
                cache = manager.getCache(name);
            }
            Ehcache backingCache = cache;
            if (!backingCache.getCacheEventNotificationService().hasCacheEventListeners()) {
                if (listeners.size() > 0) {
                    for (CacheEventListener listener : listeners) {
                        if (!backingCache.getCacheEventNotificationService().getCacheEventListeners().contains(listener)) {
                            backingCache.getCacheEventNotificationService().registerListener(listener);
                        } else {
                        }
                    }
                }
            }
            return new EhCacheWrapper(cache);
        } catch (net.sf.ehcache.CacheException e) {
            throw new CacheException("Could not create cache: " + name, e);
        }
    
    }

	@Override
	public void shutdown() {
        if (manager != null) {
            manager.shutdown();
            manager = null;
        }
	}

    public void clearAll() {
        manager.clearAll();
    }

    public CacheManager getCacheManager() {
        return manager;
    }

    public void addDefaultListener(CacheEventListener cacheEventListener) {
        listeners.add(cacheEventListener);
    }

}
