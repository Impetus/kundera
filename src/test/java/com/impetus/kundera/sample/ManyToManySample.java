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

import java.util.List;

import javax.persistence.Query;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.entity.Department;
import com.impetus.kundera.entity.Employee;

public class ManyToManySample {

	public static void main (String args[]) {
		
		Employee sm = new Employee("SM", "SM");
		Employee pm1 = new Employee("PM-1", "SM");
		Employee pm2 = new Employee("PM-2", "SM");
		sm.addtoTeam(pm1, pm2);
		pm1.setBoss(sm);
		pm2.setBoss(sm);

		Department d1 = new Department("JAVA", "1st floor");
		Department d2 = new Department(".Net", "2nd floor");
		Department d3 = new Department("PHP", "3rd floor");
		
		pm1.addtoDeptt(d2, d3);
		pm2.addtoDeptt(d1, d2);
		sm.addtoDeptt(d1, d2, d3);
		
		d1.addEmployee(sm, pm2);
		d2.addEmployee(pm1, pm2, sm);
		d3.addEmployee(pm1, sm);
		
		
		EntityManagerFactoryImpl fac = new EntityManagerFactoryImpl("test");
		EntityManagerImpl manager = (EntityManagerImpl) fac
				.createEntityManager();
		
//		manager.persist(sm);
		Department java = manager.find(Department.class, "JAVA");
		Employee java_e = java.getEmployees().get(0);
		System.out.println (java_e.getName());
		
		Query q = manager.createQuery("select d from Department d where d.address like :address");
		q.setParameter("address", "floor");
		//q.setMaxResults(2);
		List<Department> list = q.getResultList();
		
		for (Department d : list) {
			System.out.println (d.getName() + " > " + d.getEmployees().get(0));
		}
		manager.close();
		fac.close();
	}
}
