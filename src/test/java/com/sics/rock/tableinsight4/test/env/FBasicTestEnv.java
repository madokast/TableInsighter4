package com.sics.rock.tableinsight4.test.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public abstract class FBasicTestEnv {

    protected static Random random = new Random();

    protected static final Logger logger = LoggerFactory.getLogger("TEST");

}
