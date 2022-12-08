package com.csy.rpc;

import com.csy.exception.EmptyPathException;
import com.csy.exception.NumberOfParamException;
import com.csy.exception.PathAlreadyExistsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class NNServer implements RPCProtocol {

    @Override
    public void mkdirs(String pathStr) throws EmptyPathException, PathAlreadyExistsException {
        if (StringUtils.isEmpty(pathStr)) {
            throw new EmptyPathException();
        }
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "csy");
            if (fs.exists(new Path(pathStr))) {
                throw new PathAlreadyExistsException(pathStr);
            }
            fs.mkdirs(new Path(pathStr));
        } catch (IOException | URISyntaxException | InterruptedException e) {
            System.out.println(e + "");
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, NumberOfParamException {
        if (args.length != 2) {
            throw new NumberOfParamException(args, 2);
        }
        System.out.println("server init start");
        System.out.printf("address:%s, port:%s%n", args[0], args[1]);
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress(args[0])
                .setPort(Integer.parseInt(args[1]))
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();
        server.start();
        System.out.println("server init end");
    }
}