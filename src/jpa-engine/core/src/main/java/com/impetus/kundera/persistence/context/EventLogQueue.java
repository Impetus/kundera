/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.persistence.context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.persistence.context.EventLog.EventType;

/**
 * The Class EventLogQueue.
 * 
 * @author vivek.mishra
 */
class EventLogQueue
{

    /** The insert events. */
    private Map<Object, EventLog> insertEvents;

    /** The update events. */
    private Map<Object, EventLog> updateEvents;

    /** The delete events. */
    private Map<Object, EventLog> deleteEvents;

    /**
     * On event.
     * 
     * @param log
     *            the log
     * @param eventType
     *            the event type
     */
    void onEvent(EventLog log, EventType eventType)
    {

        switch (eventType)
        {
        case INSERT:

            onInsert(log);
            break;

        case UPDATE:
            onUpdate(log);
            break;

        case DELETE:
            onDelete(log);
            break;

        default:

            throw new KunderaException("Invalid event type:" + eventType);
        }

    }

    /**
     * On delete.
     * 
     * @param log
     *            the log
     */
    private void onDelete(EventLog log)
    {
        if (deleteEvents == null)
        {
            deleteEvents = new ConcurrentHashMap<Object, EventLog>();
        }

        deleteEvents.put(log.getEntityId(), log);

    }

    /**
     * On update.
     * 
     * @param log
     *            the log
     */
    private void onUpdate(EventLog log)
    {
        if (updateEvents == null)
        {
            updateEvents = new ConcurrentHashMap<Object, EventLog>();

        }

        updateEvents.put(log.getEntityId(), log);
    }

    /**
     * On insert.
     * 
     * @param log
     *            the log
     */
    private void onInsert(EventLog log)
    {
        if (insertEvents == null)
        {
            insertEvents = new ConcurrentHashMap<Object, EventLog>();
        }

        insertEvents.put(log.getEntityId(), log);

    }

    /**
     * Clear.
     */
    void clear()
    {
        if (this.insertEvents != null)
        {
            insertEvents.clear();
            insertEvents = null;

        }
        if (this.updateEvents != null)
        {
            updateEvents.clear();
            updateEvents = null;

        }
        if (this.deleteEvents != null)
        {
            deleteEvents.clear();
            deleteEvents = null;

        }
    }

    /**
     * Gets the insert events.
     * 
     * @return the insert events
     */
    Map<Object, EventLog> getInsertEvents()
    {
        return insertEvents;
    }

    /**
     * Gets the update events.
     * 
     * @return the update events
     */
    Map<Object, EventLog> getUpdateEvents()
    {
        return updateEvents;
    }

    /**
     * Gets the delete events.
     * 
     * @return the delete events
     */
    Map<Object, EventLog> getDeleteEvents()
    {
        return deleteEvents;
    }

}
