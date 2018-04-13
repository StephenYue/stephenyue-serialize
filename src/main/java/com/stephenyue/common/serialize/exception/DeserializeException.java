package com.stephenyue.common.serialize.exception;

public class DeserializeException extends RuntimeException{

    public DeserializeException(String message) {
        super(message);
    }

    public DeserializeException(String message, Throwable cause) {
        super(message, cause);
    }

}
