package com.impetus.kundera.persistence;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.query.Person;

/**
 * Junit for {@link CriteriaBuilder}
 * 
 * TODO:: 
 * 1) support for IN clause
 * 2) Composite object in where clause {Select u from CassandraPrimeUser u where u.key = :key}
 * @author vivek.mishra
 *
 */
public class KunderaCriteriaBuilderTest
{
    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();
    }

    @Test
    public void testWithoutWhereClause()
    {
        String expected = "Select p from Person p";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();

        // Check for multi column select
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testSelectedClause()
    {
        String expected = "Select p.personName from Person p";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();

        // Check for multi column select
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select((Selection) from.get("personName").alias("p"));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testCountClause()
    {
        String expected = "Select Count(p) from Person p";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();

        CriteriaQuery<Long> personQuery = criteriaBuilder.createQuery(Long.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(criteriaBuilder.count((Expression<?>) from.alias("p")));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testCountWithWhereClause()
    {
        String expected = "Select Count(p) from Person p where p.personName = \"vivek\" AND p.age = 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();

        CriteriaQuery<Long> personQuery = criteriaBuilder.createQuery(Long.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(criteriaBuilder.count((Expression<?>) from.alias("p")));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.equal(from.get("age"), 32)));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithWhereClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\"";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.equal(from.get("personName"), "vivek"));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithANDClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" AND p.age = 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.equal(from.get("age"), 32)));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithANDGTClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" AND p.age > 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.gt((Expression) from.get("age"), new Integer(32))));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithANDGTEClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" AND p.age >= 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.ge((Expression) from.get("age"), new Integer(32))));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithANDLTClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" OR p.age < 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.or(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.lt((Expression) from.get("age"), new Integer(32))));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithORLTEClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" OR p.age <= 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.or(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.le((Expression) from.get("age"), new Integer(32))));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithORClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" OR p.age = 32";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.or(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.equal(from.get("age"), 32)));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithBTWClause()
    {
        String expected = "Select p from Person p where p.age BETWEEN 10 AND 20";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.between((Expression) from.get("age"), 10, 20));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithANDBTWClause()
    {
        String expected = "Select p from Person p where p.personName = \"'vivek'\" AND p.age BETWEEN 10 AND 20";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "'vivek'"),
                criteriaBuilder.between((Expression) from.get("age"), 10, 20)));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithMultiANDClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" AND p.age = 32 AND p.salary = 3200.01";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.equal(from.get("age"), 32), criteriaBuilder.equal(from.get("salary"), 3200.01)));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testWithANDGTLTClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" AND p.age > 32 AND p.salary <= 3200.01";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.gt((Expression) from.get("age"), 32),
                criteriaBuilder.le((Expression) from.get("salary"), 3200.01)));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testOrderByWithCompositeClause()
    {
        final String expected = "Select u from PersonnelEmbedded u where u.personalDetail.phoneNo = 91234567 ORDER BY u.personalDetail.emailId ASC";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<PersonnelEmbedded> embedQuery = criteriaBuilder.createQuery(PersonnelEmbedded.class);
        Root<PersonnelEmbedded> from = embedQuery.from(PersonnelEmbedded.class);
        embedQuery.select(from.alias("u"));
        embedQuery.orderBy(criteriaBuilder.asc(from.get("personalDetail").get("emailId")));
        embedQuery.where(criteriaBuilder.equal(from.get("personalDetail").get("phoneNo"), "91234567"));

        String actual = CriteriaQueryTranslator.translate(embedQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testOrderByClause()
    {
        String expected = "Select p from Person p ORDER BY p.personName DESC";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();

        // Check for multi column select
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.orderBy(criteriaBuilder.desc(from.get("personName")));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testOrderByWithWhereClause()
    {
        String expected = "Select p from Person p where p.personName = \"vivek\" AND p.age > 32 AND p.salary <= 3200.01 ORDER BY p.personName DESC";

        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.select(from.alias("p"));
        personQuery.where(criteriaBuilder.and(criteriaBuilder.equal(from.get("personName"), "vivek"),
                criteriaBuilder.gt((Expression) from.get("age"), 32),
                criteriaBuilder.le((Expression) from.get("salary"), 3200.01)));
        personQuery.orderBy(criteriaBuilder.desc(from.get("personName")));
        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

    @Test
    public void testMultiSelectedClause()
    {
        String expected = "Select p.personName,p.age from Person p";
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();

        // Check for multi column select
        CriteriaQuery<Person> personQuery = criteriaBuilder.createQuery(Person.class);
        Root<Person> from = personQuery.from(Person.class);
        personQuery.multiselect((Selection) from.get("personName").alias("p"), (Selection) from.get("age").alias("p"));

        String actual = CriteriaQueryTranslator.translate(personQuery);
        Assert.assertEquals(expected.trim(), actual.trim());
    }

}
