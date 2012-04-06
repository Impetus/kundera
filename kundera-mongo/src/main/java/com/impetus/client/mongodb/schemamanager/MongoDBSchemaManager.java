package com.impetus.client.mongodb.schemamanager;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBSchemaManager extends AbstractSchemaManager implements
		SchemaManager {
	private Mongo m;
	private DB db;
	private DBCollection coll;
	private static final Logger logger = LoggerFactory
			.getLogger(MongoDBSchemaManager.class);

	@Override
	public void exportSchema() {
		super.exportSchema();
	}

	protected void update(List<TableInfo> tableInfos) {

	}

	protected void create(List<TableInfo> tableInfos) {

	}

	protected void create_drop(List<TableInfo> tableInfos) {
		create(tableInfos);
	}

	protected void validate(List<TableInfo> tableInfos) {
		List<String> dbNames = m.getDatabaseNames();
		boolean found = false;
		for (String dbName : dbNames) {
			if (dbName.equalsIgnoreCase(databaseName)) {
				found = true;
			}
		}
		if (!found) {
			logger.error("database " + databaseName + "does not exist");
		}

		for (TableInfo tableInfo : tableInfos) {
			if (!db.collectionExists(tableInfo.getTableName())) {
				logger.error("Collection " + tableInfo.getTableName()
						+ "does not exist in db " + db);
			}
		}
	}

	public void dropSchema() {
		if (operation != null && operation.equalsIgnoreCase("create-drop")) {
			for (TableInfo tableInfo : tableInfos) {
				coll = db.getCollection(tableInfo.getTableName());
				coll.drop();
			}
		}
	}

	protected boolean initiateClient() {
		if (kundera_client.equalsIgnoreCase("MongoDB")) {

			int localport = Integer.parseInt(port);
			try {
				db = m.getDB(puMetadata.getProperties().getProperty(
						PersistenceProperties.KUNDERA_KEYSPACE));
				m = new Mongo(host, localport);
			} catch (UnknownHostException e) {

				e.printStackTrace();
			} catch (MongoException e) {

				e.printStackTrace();
			}

			return true;
		}
		return false;
	}
}
