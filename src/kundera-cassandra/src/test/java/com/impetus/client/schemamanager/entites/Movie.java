/**
 * 
 */
package com.impetus.client.schemamanager.entites;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep Mishra
 * 
 */
@Embeddable
@IndexCollection(columns = { @Index(name = "title"), @Index(name = "released_year") })
public class Movie
{
    @Column(name = "movie_id")
    private String movieId;

    @Column(name = "title")
    private String title;

    @Column(name = "released_year")
    private String releasedYear;

    /**
     * @return the movieId
     */
    public String getMovieId()
    {
        return movieId;
    }

    /**
     * @param movieId
     *            the movieId to set
     */
    public void setMovieId(String movieId)
    {
        this.movieId = movieId;
    }

    /**
     * @return the title
     */
    public String getTitle()
    {
        return title;
    }

    /**
     * @param title
     *            the title to set
     */
    public void setTitle(String title)
    {
        this.title = title;
    }

    /**
     * @return the releasedYear
     */
    public String getReleasedYear()
    {
        return releasedYear;
    }

    /**
     * @param releasedYear
     *            the releasedYear to set
     */
    public void setReleasedYear(String releasedYear)
    {
        this.releasedYear = releasedYear;
    }
}
