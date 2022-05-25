package com.sics.rock.tableinsight4.conf;


import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value={FIELD})
public @interface FConfigItemAnnotation {

    String name();

    String description();
}


