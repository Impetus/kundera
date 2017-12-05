package com.impetus.kundera.ethereum.webapp.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
public class WebConfig extends WebMvcConfigurerAdapter
{

    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry)
    {
        registry.addResourceHandler("/*.js/**").addResourceLocations("/ui/static/");
        registry.addResourceHandler("/*.css/**").addResourceLocations("/ui/static/");
    }

}
