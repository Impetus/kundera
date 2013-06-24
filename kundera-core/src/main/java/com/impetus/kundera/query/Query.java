package com.impetus.kundera.query;

import java.util.Iterator;
import java.util.List;

public interface Query<E>
{
    void setFetchSize(int fetchsize);

    int getFetchSize();

//    E next();

//    List<E> next(int size);

    void close();
    
    Iterator<E> iterate();

}