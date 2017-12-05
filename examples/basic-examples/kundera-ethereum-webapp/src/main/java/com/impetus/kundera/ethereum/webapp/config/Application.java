package com.impetus.kundera.ethereum.webapp.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ComponentScan(basePackages = "com.impetus.kundera.ethereum.webapp")
@Import({ BeanConfig.class, WebConfig.class })
@EnableAutoConfiguration
public class Application extends SpringBootServletInitializer
{
    private static Class applicationClass = Application.class;

    public static void main(String[] args)
    {
        SpringApplication.run(Application.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application)
    {
        return application.sources(applicationClass);
    }
}
