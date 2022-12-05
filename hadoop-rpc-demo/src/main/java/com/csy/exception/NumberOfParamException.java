package com.csy.exception;

import java.util.Arrays;

public class NumberOfParamException extends Exception {

    public NumberOfParamException(String[] param, int number) {
        super(String.format("The number of parameters must be %s! param:%s", number, Arrays.toString(param)));
    }
}