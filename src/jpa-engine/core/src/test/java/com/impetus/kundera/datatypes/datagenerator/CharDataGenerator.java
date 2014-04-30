package com.impetus.kundera.datatypes.datagenerator;



public class CharDataGenerator implements DataGenerator<Character> {

	private static final char CHAR = 'a';

	@Override
	public Character randomValue() {

		return CHAR;
	}

	@Override
	public Character maxValue() {

		return 'z';
	}

	@Override
	public Character minValue() {

		return Character.MIN_VALUE;
	}

	@Override
	public Character partialValue() {

		return null;
	}

    public static void main(String[] d)
    {
        CharDataGenerator generator = new CharDataGenerator();
        String query = "Select s From StudentHBaseChar s where s.id between " + generator.minValue() + " and "
                + generator.maxValue();       
    }

}
