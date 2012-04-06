package com.impetus.client.cassandra.schemamanager;

import java.util.HashMap;

final class CassandraValidationClassMapper {

	final static HashMap<String, String> validationClassMapper = new HashMap<String, String>();

	static {

		validationClassMapper.put("class java.lang.String", "UTF8Type");
		validationClassMapper.put("char", "UTF8Type");

		validationClassMapper.put("class java.sql.Time", "IntegerType");
		validationClassMapper.put("class java.lang.Integer", "IntegerType");
		validationClassMapper.put("int", "IntegerType");
		validationClassMapper.put("class java.sql.Timestamp", "IntegerType");
		validationClassMapper.put("short", "IntegerType");
		validationClassMapper.put("class java.math.BigDecimal", "IntegerType");
		validationClassMapper.put("class java.sql.Date", "IntegerType");
		validationClassMapper.put("class java.util.Date", "IntegerType");
		validationClassMapper.put("class java.math.BigInteger", "IntegerType");

		validationClassMapper.put("class java.lang.Double", "DoubleType");
		validationClassMapper.put("double", "DoubleType");

		validationClassMapper.put("boolean", "BooleanType");

		validationClassMapper.put("class java.lang.Long", "LongType");
		validationClassMapper.put("long", "LongType");

		validationClassMapper.put("byte", "BytesType");

		validationClassMapper.put("float", "FloatType");
	}

	static String getValidationClass(String dataType) {
		String validation_class;
		validation_class = validationClassMapper.get(dataType);
		if (!(validation_class != null)) {
			validation_class = "BytesType";
		}
		return validation_class;
	}
}
