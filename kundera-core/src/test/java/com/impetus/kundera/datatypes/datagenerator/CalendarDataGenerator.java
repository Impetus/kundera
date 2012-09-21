package com.impetus.kundera.datatypes.datagenerator;

import java.util.Calendar;

public class CalendarDataGenerator implements DataGenerator<Calendar>
{
    private static final Calendar INSTANCE = Calendar.getInstance();

    @Override
    public Calendar randomValue()
    {
        return INSTANCE;
    }

    @Override
    public Calendar maxValue()
    {
        return INSTANCE;
    }

    @Override
    public Calendar minValue()
    {
        return INSTANCE;
    }

    @Override
    public Calendar partialValue()
    {
        return null;
    }

}
