package com.sics.rock.tableinsight4.conf;

/**
 * Config illegal exception
 *
 * @author zhaorx
 */
public class FConfigIllegalException extends RuntimeException {

    public FConfigIllegalException(String message) {
        super(message);
    }

    public FConfigIllegalException(String message, Throwable cause) {
        super(message, cause);
    }
}
