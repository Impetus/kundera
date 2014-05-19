package com.impetus.client.oraclenosql;

import java.lang.reflect.Field;
import java.util.Date;

import javax.persistence.PersistenceException;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordValue;

import com.impetus.kundera.property.PropertyAccessorHelper;

public final class NoSqlDBUtils
{

    static void add(final FieldDef fieldMetadata, RecordValue row, Object value, final String fieldName)
    {
        if (fieldMetadata != null)
        {
            switch (fieldMetadata.getType())
            {
            case STRING:
                
                if(value != null)
                row.put(fieldName, value.toString());
                
                break;

            case INTEGER:
                row.put(fieldName, (Integer) value);
                break;

            case LONG:
                if (value != null && value.getClass().isAssignableFrom(Date.class))
                {
                    row.put(fieldName, ((Date) value).getTime());
                } else if (value != null && value.getClass().isAssignableFrom(java.sql.Date.class))
                {
                    row.put(fieldName, ((java.sql.Date) value).getTime());
                } else if(value != null && value.getClass().isAssignableFrom(java.sql.Timestamp.class))
                {
                    row.put(fieldName, ((java.sql.Timestamp) value).getTime());
                } else if(value != null && value.getClass().isAssignableFrom(java.sql.Time.class))
                {
                    row.put(fieldName, ((java.sql.Time) value).getTime());
                }
                else
                {
                    row.put(fieldName, (Long) value);
                }

                break;

            case MAP:

                // MapValue mapValue = row.putMap(fieldName);
                // mapValue.

                break;

            case RECORD:
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            case FIXED_BINARY:
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            case DOUBLE:
                row.put(fieldName, (Double) value);
                break;

            case FLOAT:
                row.put(fieldName, (Float) value);
                break;
            /*
             * case DATE: row.put(fieldName, ((Date) value).getTime()); break;
             */
            case BOOLEAN:
                row.put(fieldName, (Boolean) value);
                break;

            case BINARY:
                if(value != null)
                row.put(fieldName, PropertyAccessorHelper.getBytes(value));
                break;

            case ARRAY:
                // TODO::
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            default:
                throw new UnsupportedOperationException("Invalid field type: " + fieldMetadata.getType() + " provided!");
            }
        }
        else
        {
            throw new PersistenceException("Invalid field " + fieldName + " provided!");
        }

    }

    static void get(final FieldDef fieldMetadata, FieldValue value, Object entity, final Field field)
    {
        if (fieldMetadata != null && !value.isNull())
        {
            switch (fieldMetadata.getType())
            {
            case STRING:
                
                    PropertyAccessorHelper.set(entity, field, value.asString().get());
                break;

            case INTEGER:
                    PropertyAccessorHelper.set(entity, field, value.asInteger().get());
                break;

            case LONG:
                    if (field.getType().isAssignableFrom(Date.class))
                    {
                        PropertyAccessorHelper.set(entity, field, new Date(value.asLong().get()));
                    }
                    else if (field.getType().isAssignableFrom(java.sql.Date.class))
                    {
                        PropertyAccessorHelper.set(entity, field, new java.sql.Date(value.asLong().get()));
                    }
                    else if (field.getType().isAssignableFrom(java.sql.Timestamp.class))
                    {
                        PropertyAccessorHelper.set(entity, field, new java.sql.Timestamp(value.asLong().get()));
                    }
                    else if (field.getType().isAssignableFrom(java.sql.Time.class))
                    {
                        PropertyAccessorHelper.set(entity, field, new java.sql.Time(value.asLong().get()));
                    }
                    else
                    {
                        PropertyAccessorHelper.set(entity, field, value.asLong().get());
                    }
                break;

            case MAP:

                // MapValue mapValue = row.putMap(fieldName);
                // mapValue.

                break;

            case RECORD:
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            case FIXED_BINARY:
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            case DOUBLE:
                    PropertyAccessorHelper.set(entity, field, value.asDouble().get());
                break;

            case FLOAT:
                    PropertyAccessorHelper.set(entity, field, value.asFloat().get());
                break;
            /*
             * case DATE: PropertyAccessorHelper.set(entity, field, new
             * Date(value.asDate().getMilliseconds())); break;
             */
            case BOOLEAN:
                    PropertyAccessorHelper.set(entity, field, value.asBoolean().get());
                break;

            case BINARY:
                    PropertyAccessorHelper.set(entity, field, value.asBinary().get());
                break;

            case ARRAY:
                // TODO::
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            default:
                throw new UnsupportedOperationException("Invalid field type: " + fieldMetadata.getType() + " provided!");
            }
        }
        else if(fieldMetadata == null)
        {
            throw new PersistenceException("Invalid field " + field.getName() + " provided!");
        }

    }

    static Object get(final FieldDef fieldMetadata, FieldValue value, final Field field)
    {
        Object retValue = null;
        if (fieldMetadata != null && !value.isNull())
        {
            switch (fieldMetadata.getType())
            {
            case STRING:
                
                retValue = value.asString().get();
                break;

            case INTEGER:
                retValue = value.asInteger().get();
                break;

            case LONG:

                retValue = value.asLong().get();

                if ( field != null && field.getType().isAssignableFrom(Date.class))
                {
                    retValue = new Date((Long) retValue);
                }else if (field != null && field.getType().isAssignableFrom(java.sql.Date.class))
                {
                    retValue = new java.sql.Date((Long) retValue);
                }
                else if(field != null && field.getType().isAssignableFrom(java.sql.Timestamp.class))
                {
                    retValue = new java.sql.Timestamp((Long) retValue);
                } else if(field != null && field.getType().isAssignableFrom(java.sql.Time.class))
                {
                    retValue = new java.sql.Time((Long) retValue);
                }
                break;

            case MAP:

                // MapValue mapValue = row.putMap(fieldName);
                // mapValue.

                break;

            case RECORD:
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            case FIXED_BINARY:
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            case DOUBLE:
                retValue = value.asDouble().get();
                break;

            /*
             * case DATE: retValue = new Date(value.asDate().getMilliseconds());
             * break;
             */
            case BOOLEAN:
                retValue = value.asBoolean().get();
                break;

            case BINARY:
                retValue = value.asBinary().get();

                break;

            case ARRAY:
                // TODO::
                throw new UnsupportedOperationException(
                        "Support for FIXED_BINARY field type is not available with Kundera");

            default:
                throw new UnsupportedOperationException("Invalid field type: " + fieldMetadata.getType() + " provided!");
            }
        }
        else if(fieldMetadata == null)
        {
            throw new PersistenceException("Invalid field " + field.getName() + " provided!");
        }

        return retValue;
    }

}
