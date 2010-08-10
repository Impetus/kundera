/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.sample;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.entity.Employee;

public class EmployeeSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		EntityManagerFactoryImpl fac = new EntityManagerFactoryImpl("test");
		EntityManagerImpl manager = (EntityManagerImpl) fac
				.createEntityManager();

		Employee sm = new Employee("SM", "SM");
		Employee pm1 = new Employee("PM-1", "SM");
		Employee pm2 = new Employee("PM-2", "SM");

		sm.addtoTeam(pm1, pm2);
		pm1.setBoss(sm);
		pm2.setBoss(sm);

		Employee sa = new Employee("SA", "SM");
		sa.setBoss(sm);
		Employee a1 = new Employee("ARC-1", "SM");
		Employee a2 = new Employee("ARC-2", "SM");

		sa.addtoTeam(a1, a2);
		a1.setBoss(sa);
		a2.setBoss(sa);

		Employee d1 = new Employee("DEV-1", "SM");
		Employee d2 = new Employee("DEV-2", "SM");
		Employee d3 = new Employee("DEV-3", "SM");
		Employee d4 = new Employee("DEV-4", "SM");
		Employee d5 = new Employee("DEV-5", "SM");
		
		pm1.addtoTeam(d1, d2);
		d1.setBoss(pm1);
		d2.setBoss(pm1);
		pm2.addtoTeam(d3, d4, d5);
		d3.setBoss(pm2);
		d4.setBoss(pm2);
		d5.setBoss(pm2);
		
		sm.addtoTeam(sa);

//		System.out.println(sm);
//		
		manager.persist(sm);
		
		Employee SA = manager.find(Employee.class, "SA");
		System.out.println ("Boss " + SA.getBoss());
		System.out.println ("Self " + SA);
	}

}
