package com.impetus.kundera.client.cassandra.dsdriver;

import com.google.common.base.Predicate;
import com.datastax.driver.core.Host;

public class HostFilterPredicate {

	private static Predicate<Host> INSTANCE = new Predicate<Host>() {

		@Override
		public boolean apply(com.datastax.driver.core.Host arg0) {
			// Dummy predicate
			return true;
		}
	};

	public Predicate<Host> getInstance() {
		return INSTANCE;
	}

}
