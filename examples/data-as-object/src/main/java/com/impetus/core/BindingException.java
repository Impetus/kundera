package com.impetus.core;

public class BindingException extends RuntimeException
{

    private static final long serialVersionUID = 1L;

    public BindingException()
    {
        super();
    }

    public BindingException(String message)
    {
        super(message);

    }

    public BindingException(Throwable cause)
    {
        super(cause);

    }

    public BindingException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
