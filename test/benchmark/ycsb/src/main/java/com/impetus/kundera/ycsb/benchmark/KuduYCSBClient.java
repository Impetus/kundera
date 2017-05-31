package com.impetus.kundera.ycsb.benchmark;

import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import static org.apache.kudu.Type.STRING;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.TimeoutException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;

public class KuduYCSBClient extends com.yahoo.ycsb.DB
{
    private static final Logger LOG = LoggerFactory.getLogger(KuduYCSBClient.class);

    private static final String KEY = "key";

    private static final int MAX_TABLETS = 9000;

    private static final long DEFAULT_SLEEP = 60000;

    private static final String SYNC_OPS_OPT = "kudu_sync_ops";

    private static final String PRE_SPLIT_NUM_TABLETS_OPT = "kudu_pre_split_num_tablets";

    private static final String TABLE_NUM_REPLICAS = "kudu_table_num_replicas";

    private static final String BLOCK_SIZE_OPT = "kudu_block_size";

    private static final String MASTER_ADDRESSES_OPT = "kudu_master_addresses";

    private static final int BLOCK_SIZE_DEFAULT = 4096;

    private static final List<String> COLUMN_NAMES = new ArrayList<>();

    private static KuduClient client;

    private static Schema schema;

    private KuduSession session;

    private KuduTable kuduTable;

    @Override
    public void init() throws DBException
    {
        String tableName = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
        initClient(tableName, getProperties());
        this.session = client.newSession();
        if (getProperties().getProperty(SYNC_OPS_OPT) != null
                && getProperties().getProperty(SYNC_OPS_OPT).equals("false"))
        {
            this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
            this.session.setMutationBufferSpace(100);
        }
        else
        {
            this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
        }
        try
        {
            this.kuduTable = client.openTable(tableName);
        }
        catch (Exception e)
        {
            throw new DBException("Could not open a table because of:", e);
        }
    }

    private static synchronized void initClient(String tableName, Properties prop) throws DBException
    {
        if (client != null)
        {
            return;
        }
        String masterAddresses = prop.getProperty(MASTER_ADDRESSES_OPT);
        if (masterAddresses == null)
        {
            masterAddresses = "quickstart.cloudera:7051";
        }
        int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
        if (numTablets > MAX_TABLETS)
        {
            throw new DBException(String.format("Specified number of tablets (%s) must be equal or below %s",
                    numTablets, MAX_TABLETS));
        }
        int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, 3);
        int blockSize = getIntFromProp(prop, BLOCK_SIZE_OPT, BLOCK_SIZE_DEFAULT);
        client = new KuduClient.KuduClientBuilder(masterAddresses).defaultSocketReadTimeoutMs(DEFAULT_SLEEP)
                .defaultOperationTimeoutMs(DEFAULT_SLEEP).defaultAdminOperationTimeoutMs(DEFAULT_SLEEP).build();
        LOG.debug("Connecting to the masters at {}", masterAddresses);
        int fieldCount = getIntFromProp(prop, CoreWorkload.FIELD_COUNT_PROPERTY,
                Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        List<ColumnSchema> columns = new ArrayList<>(fieldCount + 1);
        ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING).key(true)
                .desiredBlockSize(blockSize).build();
        columns.add(keyColumn);
        COLUMN_NAMES.add(KEY);
        for (int i = 0; i < fieldCount; i++)
        {
            String name = "field" + i;
            COLUMN_NAMES.add(name);
            columns.add(new ColumnSchema.ColumnSchemaBuilder(name, STRING).desiredBlockSize(blockSize).build());
        }
        schema = new Schema(columns);
        CreateTableOptions builder = new CreateTableOptions();
        builder.setRangePartitionColumns(new ArrayList<String>());
        List<String> hashPartitionColumns = new ArrayList<>();
        hashPartitionColumns.add(KEY);
        builder.addHashPartitions(hashPartitionColumns, numTablets);
        builder.setNumReplicas(numReplicas);
        try
        {
            client.createTable(tableName, schema, builder);
        }
        catch (Exception e)
        {
            if (!e.getMessage().contains("already exists"))
            {
                throw new DBException("Couldn't create the table", e);
            }
        }
    }

    private static int getIntFromProp(Properties prop, String propName, int defaultValue) throws DBException
    {
        String intStr = prop.getProperty(propName);
        if (intStr == null)
        {
            return defaultValue;
        }
        else
        {
            try
            {
                return Integer.valueOf(intStr);
            }
            catch (NumberFormatException ex)
            {
                throw new DBException("Provided number for " + propName + " isn't a valid integer");
            }
        }
    }

    @Override
    public void cleanup() throws DBException
    {
        try
        {
            this.session.close();
        }
        catch (Exception e)
        {
            throw new DBException("Couldn't cleanup the session", e);
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        Vector<HashMap<String, ByteIterator>> results = new Vector<>();
        final int status = scan(table, key, 1, fields, results);
        if (status != 1)
        {
            return status;
        }
        if (results.size() != 1)
        {
            return 0;
        }
        result.putAll(results.firstElement());
        return 1;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        try
        {
            KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kuduTable);
            List<String> querySchema;
            if (fields == null)
            {
                querySchema = COLUMN_NAMES;
                // No need to set the projected columns with the whole schema.
            }
            else
            {
                querySchema = new ArrayList<>(fields);
                scannerBuilder.setProjectedColumnNames(querySchema);
            }
            ColumnSchema column = schema.getColumnByIndex(0);
            KuduPredicate.ComparisonOp predicateOp = recordcount == 1 ? EQUAL : GREATER_EQUAL;
            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(column, predicateOp, startkey);
            scannerBuilder.addPredicate(predicate);
            scannerBuilder.limit(recordcount); // currently noop
            KuduScanner scanner = scannerBuilder.build();
            while (scanner.hasMoreRows())
            {
                RowResultIterator data = scanner.nextRows();
                addAllRowsToResult(data, recordcount, querySchema, result);
                if (recordcount == result.size())
                {
                    break;
                }
            }
            RowResultIterator closer = scanner.close();
            addAllRowsToResult(closer, recordcount, querySchema, result);
        }
        catch (TimeoutException te)
        {
            LOG.info("Waited too long for a scan operation with start key={}", startkey);
            return -1;
        }
        catch (Exception e)
        {
            LOG.warn("Unexpected exception", e);
            return 0;
        }
        return 1;
    }

    private void addAllRowsToResult(RowResultIterator it, int recordcount, List<String> querySchema,
            Vector<HashMap<String, ByteIterator>> result) throws Exception
    {
        RowResult row;
        HashMap<String, ByteIterator> rowResult = new HashMap<>(querySchema.size());
        if (it == null)
        {
            return;
        }
        while (it.hasNext())
        {
            if (result.size() == recordcount)
            {
                return;
            }
            row = it.next();
            int colIdx = 0;
            for (String col : querySchema)
            {
                rowResult.put(col, new StringByteIterator(row.getString(colIdx)));
                colIdx++;
            }
            result.add(rowResult);
        }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        Update update = this.kuduTable.newUpdate();
        PartialRow row = update.getRow();
        row.addString(KEY, key);
        for (int i = 1; i < schema.getColumnCount(); i++)
        {
            String columnName = schema.getColumnByIndex(i).getName();
            if (values.containsKey(columnName))
            {
                String value = values.get(columnName).toString();
                row.addString(columnName, value);
            }
        }
        apply(update);
        return 1;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        Insert insert = this.kuduTable.newInsert();
        PartialRow row = insert.getRow();
        row.addString(KEY, key);
        for (int i = 1; i < schema.getColumnCount(); i++)
        {
            row.addString(i, values.get(schema.getColumnByIndex(i).getName()).toString());
        }
        apply(insert);
        return 1;
    }

    @Override
    public int delete(String table, String key)
    {
        Delete delete = this.kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addString(KEY, key);
        apply(delete);
        return 1;
    }

    private void apply(Operation op)
    {
        try
        {
            OperationResponse response = session.apply(op);
            if (response != null && response.hasRowError())
            {
                LOG.info("Write operation failed: {}", response.getRowError());
            }
        }
        catch (KuduException ex)
        {
            LOG.warn("Write operation failed", ex);
        }
    }
}
