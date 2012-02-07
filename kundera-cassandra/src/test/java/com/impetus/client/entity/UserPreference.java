/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


/**
 * Entity class for User Preferences.
 *
 * @author amresh.singh
 */

@Entity
@Table(name = "preference", schema = "Blog")
public class UserPreference
{

    /** The preference id. */
    @Id
    String preferenceId;

    /** The website theme. */
    @Column(name = "website_theme")
    String websiteTheme;

    /** The privacy level. */
    @Column(name = "privacy_level")
    String privacyLevel; // 1, 2, 3

    /**
     * Instantiates a new user preference.
     */
    public UserPreference()
    {

    }

    /**
     * Instantiates a new user preference.
     *
     * @param prefId the pref id
     * @param theme the theme
     * @param privacyLevel the privacy level
     */
    public UserPreference(String prefId, String theme, String privacyLevel)
    {
        this.preferenceId = prefId;
        this.websiteTheme = theme;
        this.privacyLevel = privacyLevel;
    }

    /**
     * Gets the preference id.
     *
     * @return the preferenceId
     */
    public String getPreferenceId()
    {
        return preferenceId;
    }

    /**
     * Sets the preference id.
     *
     * @param preferenceId the preferenceId to set
     */
    public void setPreferenceId(String preferenceId)
    {
        this.preferenceId = preferenceId;
    }

    /**
     * Gets the website theme.
     *
     * @return the websiteTheme
     */
    public String getWebsiteTheme()
    {
        return websiteTheme;
    }

    /**
     * Sets the website theme.
     *
     * @param websiteTheme the websiteTheme to set
     */
    public void setWebsiteTheme(String websiteTheme)
    {
        this.websiteTheme = websiteTheme;
    }

    /**
     * Gets the privacy level.
     *
     * @return the privacyLevel
     */
    public String getPrivacyLevel()
    {
        return privacyLevel;
    }

    /**
     * Sets the privacy level.
     *
     * @param privacyLevel the privacyLevel to set
     */
    public void setPrivacyLevel(String privacyLevel)
    {
        this.privacyLevel = privacyLevel;
    }
}
