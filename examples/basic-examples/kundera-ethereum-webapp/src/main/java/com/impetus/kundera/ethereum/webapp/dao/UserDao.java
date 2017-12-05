//package com.impetus.kundera.ethereum.webapp.dao;
//
//import javax.persistence.EntityManager;
//import javax.persistence.EntityManagerFactory;
//import javax.persistence.PersistenceUnit;
//
//import org.springframework.stereotype.Service;
//
//import com.impetus.kundera.ethereum.webapp.model.User;
//
//@Service
//public class UserDao
//{
//    @PersistenceUnit(unitName = "cassandra_pu")
//    EntityManagerFactory entityManagerFactory;
//
//    public User addUser()
//    {
//        User user = new User();
//        user.setEmail("johndoe123@gmail.com");
//        user.setName("John Doe");
//        user.setAddress("Bangalore, Karnataka");
//        EntityManager entityManager = entityManagerFactory.createEntityManager();
//        entityManager.persist(user);
//        entityManager.close();
//        return user;
//    }
//
//    public User getUserById(String Id)
//    {
//        EntityManager entityManager = entityManagerFactory.createEntityManager();
//        User User = entityManager.find(User.class, Id);
//        return User;
//    }
//
//    public EntityManagerFactory getEntityManagerFactory()
//    {
//        return entityManagerFactory;
//    }
//
//    public void setEntityManagerFactory(EntityManagerFactory entityManagerFactory)
//    {
//        this.entityManagerFactory = entityManagerFactory;
//    }
//}
