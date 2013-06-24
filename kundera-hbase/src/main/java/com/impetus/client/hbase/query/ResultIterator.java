/**
 * 
 */
package com.impetus.client.hbase.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author impadmin
 *
 */
public class ResultIterator<E> implements Iterator<E>
{

    private HBaseClient client;
    private EntityMetadata entityMetadata;
    
    private PersistenceDelegator persistenceDelegator;
    
    
    public ResultIterator(HBaseClient client, EntityMetadata m, PersistenceDelegator pd)
    {
        this.entityMetadata = m;
        this.client = client;
        this.persistenceDelegator = pd;
    }
    @Override
    public boolean hasNext()
    {
        boolean available =client.hasNext(); 
        if(!available)
        {
            client.reset();
        }
        
        return available;
    }

    @Override
    public E next()
    {
            E result = (E) client.next(entityMetadata);
            if (entityMetadata.isRelationViaJoinTable()
                    && (entityMetadata.getRelationNames() == null || (entityMetadata.getRelationNames().isEmpty())))
            {
                result = setRelationEntities(result, client, entityMetadata);
            }
            return result;
    }

    @Override
    public void remove()
    {
         throw new UnsupportedOperationException("remove() over result iterator is not supported");
    }

    
    private E setRelationEntities(Object enhanceEntity, Client client, EntityMetadata m)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        E result = null;
        if (enhanceEntity != null)
        {
              
                if (!(enhanceEntity instanceof EnhanceEntity))
                {
                    enhanceEntity = new EnhanceEntity(enhanceEntity, PropertyAccessorHelper.getId(enhanceEntity, m), null);
                }

                EnhanceEntity ee = (EnhanceEntity) enhanceEntity;

                result = (E) client.getReader().recursivelyFindEntities(ee.getEntity(), ee.getRelations(), m,
                        persistenceDelegator);
        }

        return result;
    }
}
