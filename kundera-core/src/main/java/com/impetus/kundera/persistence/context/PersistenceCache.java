/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence.context;



/**
 * Implementation of Persistence Context as defined in JPA.
 * Acts as a cache of entities. 
 * @author amresh.singh
 */
public class PersistenceCache
{
    /* Main Cache of entity objects */
    private CacheBase mainCache;
    
    /* Cache of embedded objects */
    private CacheBase embeddedCache;
    
    /* Cache of objects within element collection */
    private CacheBase elementCollectionCache;
    
    /* Cache of transactional objects */
    private CacheBase transactionalCache;
    
    FlushStackManager flushStackManager;
    
    /** one instance of this class */
    public static final PersistenceCache INSTANCE = new PersistenceCache();
    
    /**
     * Stack containing Nodes to be flushed
     * Entities are always flushed from the top, there way to bottom until stack is empty 
     */
    private FlushStack flushStack;   
    
    public PersistenceCache() {
        initialize();
    }
    
    private void initialize() {
        mainCache = new MainCache();
        embeddedCache = new EmbeddedCache();
        elementCollectionCache = new ElementCollectionCache();
        transactionalCache = new TransactionalCache();
        
        flushStack = new FlushStack();
        
        flushStackManager = new FlushStackManager();
    }    
    
    public void clean() {
        mainCache = null;
        embeddedCache = null;
        elementCollectionCache = null;
        transactionalCache = null;
        
        flushStack.clear(); flushStack = null;
        flushStackManager = null;
    }
    
    /**
     * @return the mainCache
     */
    public CacheBase getMainCache()
    {
        return mainCache;
    } 



    /**
     * @param mainCache the mainCache to set
     */
    public void setMainCache(CacheBase mainCache)
    {
        this.mainCache = mainCache;
    }



    /**
     * @return the embeddedCache
     */
    public CacheBase getEmbeddedCache()
    {
        return embeddedCache;
    }



    /**
     * @param embeddedCache the embeddedCache to set
     */
    public void setEmbeddedCache(CacheBase embeddedCache)
    {
        this.embeddedCache = embeddedCache;
    }



    /**
     * @return the elementCollectionCache
     */
    public CacheBase getElementCollectionCache()
    {
        return elementCollectionCache;
    }



    /**
     * @param elementCollectionCache the elementCollectionCache to set
     */
    public void setElementCollectionCache(CacheBase elementCollectionCache)
    {
        this.elementCollectionCache = elementCollectionCache;
    }



    /**
     * @return the transactionalCache
     */
    public CacheBase getTransactionalCache()
    {
        return transactionalCache;
    }



    /**
     * @param transactionalCache the transactionalCache to set
     */
    public void setTransactionalCache(CacheBase transactionalCache)
    {
        this.transactionalCache = transactionalCache;
    }



    /**
     * @return the flushStack
     */
    public FlushStack getFlushStack()
    {
        return flushStack;
    }



    /**
     * @param flushStack the flushStack to set
     */
    public void setFlushStack(FlushStack flushStack)
    {
        this.flushStack = flushStack;
    }    

}
