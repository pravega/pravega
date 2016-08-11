package com.emc.pravega.controller.server;

import org.apache.thrift.TException;

/**
 * Created by root on 8/11/16.
 */
public class EchoServiceImpl implements com.emc.pravega.controller.api.EchoService.Iface {
    @Override
    public String echo(String req) throws TException {
        System.out.println("Echo Method invoked");
        return "echo:" + req;
    }
}
