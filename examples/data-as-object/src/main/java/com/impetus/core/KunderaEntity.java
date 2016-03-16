package com.impetus.core;

import java.util.List;

public interface KunderaEntity<T, K>
{

    T find(K key);

    void save();

    void update();

    void delete();

    List<T> query(String query);
    
    List<T> query(String query, QueryType type);

    List<T> leftJoin(Class clazz, String joinColumn, String... columnTobeFetched);

}
