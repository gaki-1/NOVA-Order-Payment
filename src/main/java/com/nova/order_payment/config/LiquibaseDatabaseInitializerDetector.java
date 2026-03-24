package com.nova.order_payment.config;

import liquibase.integration.spring.SpringLiquibase;
import org.springframework.boot.sql.init.dependency.AbstractBeansOfTypeDatabaseInitializerDetector;

import java.util.Set;

public class LiquibaseDatabaseInitializerDetector extends AbstractBeansOfTypeDatabaseInitializerDetector {

    @Override
    protected Set<Class<?>> getDatabaseInitializerBeanTypes() {
        return Set.of(SpringLiquibase.class);
    }
}
