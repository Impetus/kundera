package com.impetus.kundera.datatypes.datagenerator;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateDataGenerator implements DataGenerator<Date> {
	private static final LongDataGenerator LONG_DATA_GENERATOR = new LongDataGenerator();

	private static final Date MIN_DATE = Calendar.getInstance().getTime();

	private static final Date MAX_DATE = new Date(Long.MAX_VALUE/100000);

	private static final Date DATE = new Date(System.currentTimeMillis());

	@Override
	public Date randomValue() {
		return DATE;
	}

	@Override
	public Date maxValue() {
		return new Date(Integer.MAX_VALUE);
	}

	@Override
	public Date minValue() {
		return new Date(1234569L);
	}

	@Override
	public Date partialValue() {

		return null;
	}

}
