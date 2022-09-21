package com.sics.rock.tableinsight4.conf;

/**
 * Config parse exception
 *
 * @author zhaorx
 */
public class FConfigParseException extends RuntimeException {

    public FConfigParseException(String message) {
        super(message);
    }

    public FConfigParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
