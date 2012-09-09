/**
 * 
 */
package com.impetus.client.mongodb.utils;

import java.util.Properties;

import javax.net.SocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.kundera.configure.KunderaClientProperties;
import com.impetus.kundera.configure.KunderaClientProperties.DataStore;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.MongoOptions;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoDBUtils
{
    private static Logger logger = LoggerFactory.getLogger(MongoDBUtils.class);

    public static void populateMongoOptions(MongoOptions mo, Properties props)
    {
        if (props != null)
        {
            if (props.get(MongoDBConstants.DB_DECODER_FACTORY) != null)
            {
                mo.setDbDecoderFactory((DBDecoderFactory) props.get(MongoDBConstants.DB_DECODER_FACTORY));
            }
            else
            {
                mo.setDbDecoderFactory(DefaultDBDecoder.FACTORY);
            }
            if (props.get(MongoDBConstants.DB_ENCODER_FACTORY) != null)
            {
                mo.setDbEncoderFactory((DBEncoderFactory) props.get(MongoDBConstants.DB_ENCODER_FACTORY));
            }
            else
            {
                mo.setDbEncoderFactory(DefaultDBEncoder.FACTORY);
            }

            mo.setAutoConnectRetry(Boolean.parseBoolean((String) props.get(MongoDBConstants.AUTO_CONNECT_RETRY)));
            mo.setFsync(Boolean.parseBoolean((String) props.get(MongoDBConstants.FSYNC)));
            mo.setJ(Boolean.parseBoolean((String) props.get(MongoDBConstants.J)));

            if (props.get(MongoDBConstants.SAFE) != null)
            {
                mo.setSafe((Boolean) props.get(MongoDBConstants.SAFE));
            }
            if (props.get(MongoDBConstants.SOCKET_FACTORY) != null)
            {
                mo.setSocketFactory((SocketFactory) props.get(MongoDBConstants.SOCKET_FACTORY));
            }
            else
            {
                mo.setSocketFactory(SocketFactory.getDefault());
            }

            try
            {
                if (props.get(MongoDBConstants.CONNECTION_PER_HOST) != null)
                {
                    mo.setConnectTimeout(Integer.parseInt((String) props.get(MongoDBConstants.CONNECT_TIME_OUT)));
                }
                if (props.get(MongoDBConstants.MAX_WAIT_TIME) != null)
                {
                    mo.setMaxWaitTime(Integer.parseInt((String) props.get(MongoDBConstants.MAX_WAIT_TIME)));
                }
                else
                {
                    mo.setMaxWaitTime(MongoDBConstants.DEFAULT_MAX_WAIT_TIME);
                }
                if (props.get(MongoDBConstants.TABCM) != null)
                {
                    mo.setThreadsAllowedToBlockForConnectionMultiplier(Integer.parseInt((String) props
                            .get(MongoDBConstants.TABCM)));
                }
                else
                {
                    mo.setThreadsAllowedToBlockForConnectionMultiplier(MongoDBConstants.DEFAULT_TABCM);
                }
                if (props.get(MongoDBConstants.W) != null)
                {
                    mo.setW(Integer.parseInt((String) props.get(MongoDBConstants.W)));
                }
                if (props.get(MongoDBConstants.W_TIME_OUT) != null)
                {
                    mo.setWtimeout(Integer.parseInt((String) props.get(MongoDBConstants.W_TIME_OUT)));
                }
                if (props.get(MongoDBConstants.MAX_AUTO_CONNECT_RETRY) != null)
                {
                    mo.setMaxAutoConnectRetryTime(Long.parseLong((String) props
                            .get(MongoDBConstants.MAX_AUTO_CONNECT_RETRY)));
                }
            }
            catch (NumberFormatException nfe)
            {
                logger.warn("Error while setting mongo properties, caused by :" + nfe);
            }
        }
    }

    public static DataStore getDataStoreInfo(String pu)
    {
        KunderaClientProperties kProperties = pu != null ? KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getSchemaMetadata().getConfigurationProperties().get(pu) : null;
        PersistenceUnitMetadata puMetadata = pu != null ? KunderaMetadataManager.getPersistenceUnitMetadata(pu) : null;
        if (kProperties != null && kProperties.getDatastores() != null)
        {
            for (DataStore dataStore : kProperties.getDatastores())
            {
                if (dataStore != null && dataStore.getName() != null && puMetadata != null
                        && puMetadata.getClient() != null
                        && puMetadata.getClient().equalsIgnoreCase(dataStore.getName()))
                {
                    return dataStore;
                }
            }
        }
        return null;
    }
}
