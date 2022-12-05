package com.csy.rpc;

import com.csy.exception.EmptyPathException;
import com.csy.exception.PathAlreadyExistsException;

/**
 * 定义RPC接口类
 */
public interface RPCProtocol {
    long versionID = 666;

    /**
     * 创建目录
     */
    void mkdirs(String path) throws EmptyPathException, PathAlreadyExistsException;
}