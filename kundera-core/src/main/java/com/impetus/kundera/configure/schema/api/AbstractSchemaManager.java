/**
 * 
 */
package com.impetus.kundera.configure.schema.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.sun.xml.internal.ws.util.MetadataUtil;

/**
 * @author impadmin
 * 
 */
public abstract class AbstractSchemaManager {

	protected PersistenceUnitMetadata puMetadata;
	protected String port;
	protected String host;
	protected String kundera_client;
	protected String databaseName;
	protected List<TableInfo> tableInfos;
	protected String operation;
	protected boolean useSecondryIndex = false;

	// @Override
	protected void exportSchema() {

		ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE
				.getApplicationMetadata();

		// Actually, start 1 pu.
		Map<String, List<TableInfo>> puToSchemaCol = appMetadata
				.getSchemaMetadata().getPuToSchemaCol();
		Set<String> pus = puToSchemaCol.keySet();
		for (String pu : pus) {

			puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
					.getPersistenceUnitMetadata(pu);

			kundera_client = puMetadata.getProperties().getProperty(
					PersistenceProperties.KUNDERA_CLIENT);
			port = puMetadata.getProperties().getProperty(
					PersistenceProperties.KUNDERA_PORT);
			host = puMetadata.getProperties().getProperty(
					PersistenceProperties.KUNDERA_NODES);
			databaseName = puMetadata.getProperties().getProperty(
					PersistenceProperties.KUNDERA_KEYSPACE);
			useSecondryIndex = MetadataUtils.useSecondryIndex(pu);

			if (initiateClient()) {
				tableInfos = puToSchemaCol.get(pu);
				handleOperations(tableInfos);
				// this.persistenceUnit = pu;
			}

		}
	}

	protected abstract boolean initiateClient();

	protected abstract void validate(List<TableInfo> tableInfos);

	protected abstract void update(List<TableInfo> tableInfos);

	protected abstract void create(List<TableInfo> tableInfos);

	protected abstract void create_drop(List<TableInfo> tableInfos);

	private void handleOperations(List<TableInfo> tableInfos) {
		String autoddl = puMetadata
				.getProperty(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
		if (autoddl.equalsIgnoreCase("create-drop")) {
			create_drop(tableInfos);
		} else if (autoddl.equalsIgnoreCase("create")) {
			create(tableInfos);
		} else if (autoddl.equalsIgnoreCase("update")) {
			update(tableInfos);
		} else if (autoddl.equalsIgnoreCase("validate")) {
			validate(tableInfos);
		}
		operation = autoddl;
	}

	// enum ScheamOperation {
	// createdrop, create, validate, update,
	//
	// }
	//
	// private ScheamOperation getInstance(String operation) {
	// if (operation.equalsIgnoreCase("create-drop")) {
	// operation = "createdrop";
	// }
	//
	// return ScheamOperation.valueOf(operation);
	// }
}
