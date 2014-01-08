package com.impetus.kundera.datatypes.datagenerator;

import java.util.Date;

public class DateDataGenerator implements DataGenerator<Date> {
	private static final LongDataGenerator LONG_DATA_GENERATOR = new LongDataGenerator();

	private static final Date MIN_DATE = new Date(0L);

	private static final Date MAX_DATE = new Date(Long.MAX_VALUE/100000);

	private static final Date DATE = new Date(LONG_DATA_GENERATOR.randomValue() + 31536000000l);

	@Override
	public Date randomValue() {
		return DATE;
	}

	@Override
	public Date maxValue() {
		return MAX_DATE;
	}

	@Override
	public Date minValue() {
		return MIN_DATE;
	}

	@Override
	public Date partialValue() {

		return null;
	}

}
