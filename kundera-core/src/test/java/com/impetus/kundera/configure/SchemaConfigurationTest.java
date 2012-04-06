package com.impetus.kundera.configure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

public class SchemaConfigurationTest {
	private SchemaConfiguration configuration;
	private Map<String, List<TableInfo>> puToSchemaCol;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		configuration = new SchemaConfiguration("cassandra");
		puToSchemaCol = new HashMap<String, List<TableInfo>>();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConfigure() {
		getEntityManagerFactory();
		configuration.configure();
		puToSchemaCol = configuration.getPuToSchemaCol();
		Assert.assertEquals(1, puToSchemaCol.size());
		Assert.assertNotNull(puToSchemaCol.containsKey("cassandra"));
		List<TableInfo> tableInfos = puToSchemaCol.get("cassandra");
		Assert.assertNotNull(tableInfos);
	
		Assert.assertEquals(8, tableInfos.size());
		
		
		// Assert.assertEquals("class java.lang.String", tableInfos.get(0)
		// .getTableIdType());
		// Assert.assertEquals("PERSONNEL", tableInfos.get(0).getTableName());
		// Assert.assertEquals("Super", tableInfos.get(0).getType());
		// List<EmbeddedColumnInfo> columnInfos = tableInfos.get(0)
		// .getEmbeddedColumnMetadatas();
		// Assert.assertEquals(2, columnInfos.size());

		// Assert.assertEquals("class java.lang.String", columnInfos.get(0)
		// .getType());
		// Assert.assertEquals(false, columnInfos.get(0).isIndexable());

	}

	private EntityManagerFactoryImpl getEntityManagerFactory() {
		Map<String, Object> props = new HashMap<String, Object>();
		String persistenceUnit = "cassandra";
		props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
		props.put(PersistenceProperties.KUNDERA_CLIENT, "pelops");
		props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
		props.put(PersistenceProperties.KUNDERA_PORT, "9160");
		props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaCoreExmples");
		props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
		ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE
				.getApplicationMetadata();
		PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
		puMetadata.setPersistenceUnitName(persistenceUnit);
		Properties p = new Properties();
		p.putAll(props);
		puMetadata.setProperties(p);
		Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
		metadata.put("cassandra", puMetadata);
		appMetadata.addPersistenceUnitMetadata(metadata);

		Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

		List<String> pus = new ArrayList<String>();
		pus.add(persistenceUnit);
		clazzToPu.put(CoreEntitySimple.class.getName(), pus);
		clazzToPu.put(CoreEntitySuper.class.getName(), pus);
		clazzToPu.put(CoreEntityAddressUni1To1.class.getName(), pus);
		clazzToPu.put(CoreEntityAddressUni1ToM.class.getName(), pus);
		clazzToPu.put(CoreEntityAddressUniMTo1.class.getName(), pus);
		clazzToPu.put(CoreEntityPersionUniMto1.class.getName(), pus);
		clazzToPu.put(CoreEntityPersonUni1To1.class.getName(), pus);
		clazzToPu.put(CoreEntityPersonUni1ToM.class.getName(), pus);

		appMetadata.setClazzToPuMap(clazzToPu);

		EntityMetadata m = new EntityMetadata(CoreEntitySimple.class);
		EntityMetadata m1 = new EntityMetadata(CoreEntitySuper.class);
		EntityMetadata m2 = new EntityMetadata(CoreEntityAddressUni1To1.class);
		EntityMetadata m3 = new EntityMetadata(CoreEntityAddressUni1ToM.class);
		EntityMetadata m4 = new EntityMetadata(CoreEntityAddressUniMTo1.class);
		EntityMetadata m5 = new EntityMetadata(CoreEntityPersionUniMto1.class);
		EntityMetadata m6 = new EntityMetadata(CoreEntityPersonUni1To1.class);
		EntityMetadata m7 = new EntityMetadata(CoreEntityPersonUni1ToM.class);

		TableProcessor processor = new TableProcessor();
		processor.process(CoreEntitySimple.class, m);
		processor.process(CoreEntitySuper.class, m1);
		processor.process(CoreEntityAddressUni1To1.class, m2);
		processor.process(CoreEntityAddressUni1ToM.class, m3);
		processor.process(CoreEntityAddressUniMTo1.class, m4);
		processor.process(CoreEntityPersionUniMto1.class, m5);
		processor.process(CoreEntityPersonUni1To1.class, m6);
		processor.process(CoreEntityPersonUni1ToM.class, m7);

		m.setPersistenceUnit(persistenceUnit);
		m1.setPersistenceUnit(persistenceUnit);
		m2.setPersistenceUnit(persistenceUnit);
		m3.setPersistenceUnit(persistenceUnit);
		m4.setPersistenceUnit(persistenceUnit);
		m5.setPersistenceUnit(persistenceUnit);
		m6.setPersistenceUnit(persistenceUnit);
		m7.setPersistenceUnit(persistenceUnit);

		MetamodelImpl metaModel = new MetamodelImpl();
		metaModel.addEntityMetadata(CoreEntitySimple.class, m);
		metaModel.addEntityMetadata(CoreEntitySuper.class, m1);
		metaModel.addEntityMetadata(CoreEntityAddressUni1To1.class, m2);
		metaModel.addEntityMetadata(CoreEntityAddressUni1ToM.class, m3);
		metaModel.addEntityMetadata(CoreEntityAddressUniMTo1.class, m4);
		metaModel.addEntityMetadata(CoreEntityPersionUniMto1.class, m5);
		metaModel.addEntityMetadata(CoreEntityPersonUni1To1.class, m6);
		metaModel.addEntityMetadata(CoreEntityPersonUni1ToM.class, m7);

		appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
		return null;
	}

}
