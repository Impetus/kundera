//package com.impetus.kudu.client.crud;
//
//import java.util.List;
//
//import javax.persistence.EntityManager;
//import javax.persistence.EntityManagerFactory;
//import javax.persistence.Persistence;
//import javax.persistence.Query;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.impetus.kudu.client.entities.Person;
//
//public class KuduQueryTest {
//    private static final String PU = "kudu_pu";
//    private static EntityManagerFactory emf;
//    private static EntityManager em;
//
//    @BeforeClass
//    public static void setUp() {
//        emf = Persistence.createEntityManagerFactory(PU);
//        em = emf.createEntityManager();
//    }
//
//    @Test
//    public void testSelect() {
//        Query query = em.createQuery("Select p.age, p.salary from Person p where p.age >= 22");
//        List<Person> results = query.getResultList();
//        System.out.println(results.size());
//
//        query = em.createQuery("Select p.personName from Person p");
//        results = query.getResultList();
//        System.out.println(results.size());
//
//        query = em.createQuery("Select p from Person p where p.personId = 'qwerty'");
//        results = query.getResultList();
//        System.out.println(results.size());
//
//        query = em.createQuery("Select p.age, p.salary from Person p where p.age >= 10 and p.age <= 100");
//        results = query.getResultList();
//        System.out.println(results.size());
//    }
//}
