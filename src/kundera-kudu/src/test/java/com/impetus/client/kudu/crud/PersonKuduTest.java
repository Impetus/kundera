//package com.impetus.client.kudu.crud;
//
//import javax.persistence.EntityManager;
//import javax.persistence.EntityManagerFactory;
//import javax.persistence.Persistence;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.impetus.client.kudu.entities.Person;
//
//public class PersonKuduTest {
//	
//	private static final String PU = "kudu_pu";
//	private static EntityManagerFactory emf;
//	private static EntityManager em;
//	
//	@BeforeClass
//	public static void setUp(){
//		emf = Persistence.createEntityManagerFactory(PU);
//		em = emf.createEntityManager();
//	}
//	
//	@Test
//	public void testInsert(){ //working
//		Person person1 = getPerson("qwerty", "dev", 22, 30000.5);
//        em.persist(person1);
//	}
//	
//	@Test
//	public void testFind(){ //working
//        Person p = em.find(Person.class, "2");
//        System.out.println(p.getPersonId());
//	}
//	
//	@Test
//	public void testMerge(){ //working
//		Person p = em.find(Person.class, "2");
//        p.setPersonName("kart_upd");
//        em.merge(p);
//	}
//	
//	@Test
//	public void testDelete(){ //working
//		Person person1 = getPerson("kadbfka", "dev", 22, 30000.5);
//        em.remove(person1);
//	}
//	
//	 private Person getPerson(String id, String name, Integer age, Double salary)
//	    {
//	        Person person = new Person();
//	        person.setAge(age);
//	        person.setPersonId(id);
//	        person.setPersonName(name);
//	        person.setSalary(salary);
//	        return person;
//	    }
//	
//}
