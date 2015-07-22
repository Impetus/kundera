package com.impetus.client.generatedId;

import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;

public class CustomIdGenerator implements AutoGenerator, TableGenerator
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CustomIdGenerator.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.AutoGenerator#generate(com.impetus.kundera
     * .client.Client, java.lang.Object)
     */
    @Override
    public Object generate(Client<?> client, String dataType)
    {
               
            return java.util.UUID.randomUUID();

        
    }

    @Override
    public Object generate(TableGeneratorDiscriptor discriptor, ClientBase client, String dataType)
    {
        Cassandra.Client conn = ((CassandraClientBase) client).getRawClient(discriptor.getSchema());
        long latestCount = 0l;
        Random random = new Random();
        if (latestCount == 0)
        {
            return (long) discriptor.getInitialValue();
        }
        else
        {
            latestCount = random.nextLong();
            return (latestCount + 1) * discriptor.getAllocationSize();
        }
    }

   


}
