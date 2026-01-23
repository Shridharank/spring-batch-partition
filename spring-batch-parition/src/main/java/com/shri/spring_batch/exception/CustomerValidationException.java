package com.shri.spring_batch.exception;

public class CustomerValidationException extends RuntimeException{

    public CustomerValidationException(String message) {
        super(message);
    }
}
