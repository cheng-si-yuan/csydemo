package com.csy.exception;

public class EmptyPathException extends Exception {

    public EmptyPathException() {
        super("Path must not be empty!");
    }
}