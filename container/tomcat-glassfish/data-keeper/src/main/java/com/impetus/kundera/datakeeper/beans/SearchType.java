/**
 * 
 */
package com.impetus.kundera.datakeeper.beans;


/**
 * @author Kuldeep.Mishra
 * 
 */
enum SearchType
{
    ID, NAME;

    static SearchType getSearchType(int searchBy)
    {
        if (searchBy == 1)
        {
            return ID;
        }
        else if (searchBy == 0)
        {
            return NAME;
        }
        return null;
    }
}
