package com.csy.exception;

public class PathAlreadyExistsException extends Exception {

    public PathAlreadyExistsException(String path) {
        super("Path already exists: " + path);
    }
}