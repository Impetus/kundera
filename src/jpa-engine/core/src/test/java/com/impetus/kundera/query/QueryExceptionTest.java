/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.query;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.cache.CacheException;
import com.impetus.kundera.classreading.ResourceReadingException;
import com.impetus.kundera.client.ClientResolverException;
import com.impetus.kundera.configure.PersistenceUnitConfigurationException;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.index.IndexingException;
import com.impetus.kundera.index.LuceneIndexingException;
import com.impetus.kundera.loader.ClientLoaderException;
import com.impetus.kundera.loader.KunderaAuthenticationException;
import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.loader.PersistenceLoaderException;
import com.impetus.kundera.metadata.model.ApplicationLoaderException;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.persistence.KunderaTransactionException;
import com.impetus.kundera.persistence.event.EventListenerException;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.proxy.LazyInitializationException;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * @author vivek.mishra junit for {@link QueryHandlerException},
 *         {@link JPQLParseException}, {@link EntityReaderException} etc. Somehow
 *         coverage is not getting count. So this junit is purly from coverage
 *         purpose only.
 */
public class QueryExceptionTest
{

    @Test
    public void testQueryHandlerException()
    {
        QueryHandlerException exception = new QueryHandlerException();
        Assert.assertNotNull(exception);

        exception = new QueryHandlerException("Error with string");
        Assert.assertNotNull(exception);
        exception = new QueryHandlerException("Error with string and runtime error", new RuntimeException());
        Assert.assertNotNull(exception);
        exception = new QueryHandlerException(new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testJPQLParseException()
    {
        JPQLParseException exception = new JPQLParseException();
        Assert.assertNotNull(exception);

        exception = new JPQLParseException("Error with string");
        Assert.assertNotNull(exception);
        exception = new JPQLParseException(new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testLazyInitializationException()
    {
        LazyInitializationException exception = new LazyInitializationException("Error with string");
        Assert.assertNotNull(exception);
        exception = new LazyInitializationException(new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testEntityReaderException()
    {
        EntityReaderException exception = new EntityReaderException();
        Assert.assertNotNull(exception);

        exception = new EntityReaderException("Error with string");
        Assert.assertNotNull(exception);
        exception = new EntityReaderException(new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testKunderaTransactionException()
    {
        KunderaTransactionException exception = new KunderaTransactionException();
        Assert.assertNotNull(exception);

        exception = new KunderaTransactionException("Error with string");
        Assert.assertNotNull(exception);
        exception = new KunderaTransactionException(new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new KunderaTransactionException("Error with string", new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new KunderaTransactionException("Error with string", new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testMetaModelLoaderException()
    {
        MetamodelLoaderException exception = new MetamodelLoaderException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new MetamodelLoaderException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new MetamodelLoaderException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new MetamodelLoaderException();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testClientLoaderException()
    {
        ClientLoaderException exception = new ClientLoaderException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new ClientLoaderException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new ClientLoaderException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new ClientLoaderException();
        Assert.assertNotNull(exception);
    }


    @Test
    public void testPersistenceLoaderException()
    {
        PersistenceLoaderException exception = new PersistenceLoaderException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new PersistenceLoaderException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new PersistenceLoaderException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new PersistenceLoaderException();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testKunderaAuthenticationException()
    {
        KunderaAuthenticationException exception = new KunderaAuthenticationException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new KunderaAuthenticationException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new KunderaAuthenticationException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new KunderaAuthenticationException();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testLuceneIndexingException()
    {
        LuceneIndexingException exception = new LuceneIndexingException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new LuceneIndexingException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new LuceneIndexingException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new LuceneIndexingException();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testIndexingException()
    {
        IndexingException exception = new IndexingException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new IndexingException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new IndexingException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new IndexingException();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testInvalidEntityDefintionException()
    {
        InvalidEntityDefinitionException exception = new InvalidEntityDefinitionException("Error with string");
        Assert.assertNotNull(exception);
    
        exception = new InvalidEntityDefinitionException();
        Assert.assertNotNull(exception);
    }
    
    @Test
    public void testInvalidConfigurationException()
    {
        InvalidConfigurationException exception = new InvalidConfigurationException("Error with string");
        Assert.assertNotNull(exception);
    

        exception = new InvalidConfigurationException(new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new InvalidConfigurationException();
        Assert.assertNotNull(exception);        
        
        exception = new InvalidConfigurationException("Error with string", new RuntimeException());
        Assert.assertNotNull(exception);
    }
    
    @Test
    public void testResourceReadingException()
    {
        ResourceReadingException exception = new com.impetus.kundera.classreading.ResourceReadingException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new ResourceReadingException(new RuntimeException());
        Assert.assertNotNull(exception);
    
        exception = new ResourceReadingException("KunderaTests",new RuntimeException());
        Assert.assertNotNull(exception);
        
        exception = new ResourceReadingException();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testSchemaGenerationException()
    {

        SchemaGenerationException exception = new SchemaGenerationException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new SchemaGenerationException(new RuntimeException());
        Assert.assertNotNull(exception);

    
        exception = new SchemaGenerationException(new RuntimeException(),"KunderaTests");
        Assert.assertNotNull(exception);

        exception = new SchemaGenerationException(new RuntimeException(),"KunderaTests","test");
        Assert.assertNotNull(exception);

        exception = new SchemaGenerationException(new RuntimeException(),"KunderaTests","test");
        Assert.assertNotNull(exception);

        exception = new SchemaGenerationException("error with string ",new RuntimeException(),"KunderaTests","test");
        Assert.assertNotNull(exception);
        
        exception = new SchemaGenerationException("error with string ", "cassandra", "KunderaTests","test");
        Assert.assertNotNull(exception);

        exception = new SchemaGenerationException("error with string ","KunderaTests","test");
        Assert.assertNotNull(exception);

        exception = new SchemaGenerationException("error with string ",new RuntimeException(),"KunderaTests");
        Assert.assertNotNull(exception);
    }

    @Test
    public void testPersistenceUnitConfigurationException()
    {
        PersistenceUnitConfigurationException exception = new PersistenceUnitConfigurationException("Error with string");
        Assert.assertNotNull(exception);
        
        exception = new PersistenceUnitConfigurationException(new RuntimeException());
        Assert.assertNotNull(exception);   

        exception = new PersistenceUnitConfigurationException();
        Assert.assertNotNull(exception);

        exception = new PersistenceUnitConfigurationException("KunderaTests", new RuntimeException());
        Assert.assertNotNull(exception);

        exception = new PersistenceUnitConfigurationException("error with string ");
        Assert.assertNotNull(exception);

        exception = new PersistenceUnitConfigurationException(new RuntimeException());
        Assert.assertNotNull(exception);
    }

    @Test
    public void testApplicationLoaderException()
    {
        ApplicationLoaderException exception = new ApplicationLoaderException("Error with string");
        Assert.assertNotNull(exception);        
    }
    
    @Test
    public void testPropertyAccessException()
    {
        PropertyAccessException exception = new PropertyAccessException("Error with string");
        Assert.assertNotNull(exception); 
        
        exception = new PropertyAccessException();
        Assert.assertNotNull(exception);   
        
        exception = new PropertyAccessException(new RuntimeException());
        Assert.assertNotNull(exception);   
    }
    
    @Test
    public void testEventListenerException()
    {
        EventListenerException exception = new EventListenerException("Error with string");
        Assert.assertNotNull(exception); 
        
        exception = new EventListenerException("Error with string", new RuntimeException());
        Assert.assertNotNull(exception); 
        
        exception = new EventListenerException(new RuntimeException());
        Assert.assertNotNull(exception); 
        
        exception = new EventListenerException();
        Assert.assertNotNull(exception);      
       
    }
    
    @Test
    public void testCacheException()
    {
        CacheException exception = new CacheException("Error with string");
        Assert.assertNotNull(exception); 
        
        exception = new CacheException("Error with string", new RuntimeException());
        Assert.assertNotNull(exception); 
        
        exception = new CacheException(new RuntimeException());
        Assert.assertNotNull(exception); 
        
       
    }
    
    @Test
    public void testClientResolverException()
    {
        ClientResolverException exception = new ClientResolverException("Error with string");
        Assert.assertNotNull(exception); 
        
        exception = new ClientResolverException("Error with string", new RuntimeException());
        Assert.assertNotNull(exception); 
        
        exception = new ClientResolverException(new RuntimeException());
        Assert.assertNotNull(exception); 
        
        exception = new ClientResolverException();
        Assert.assertNotNull(exception); 
    }
        
       
        
       
    
    
    
    
}
