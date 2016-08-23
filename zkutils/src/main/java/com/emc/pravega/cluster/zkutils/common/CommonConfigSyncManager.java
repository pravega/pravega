package com.emc.pravega.cluster.zkutils.common;

import com.emc.pravega.cluster.zkutils.abstraction.ConfigChangeListener;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManager;
import com.emc.pravega.cluster.zkutils.jsonhelper.JSONHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kandha on 8/23/16.
 */
public abstract class CommonConfigSyncManager implements ConfigSyncManager {

    private static final String HOST_STRING = "\"host\"";
    private static final String PORT_STRING = "\"port\"";
    private static final String METADATA_STRING = "\"metadata\"";
    protected final ConfigChangeListener listener;



    public CommonConfigSyncManager(ConfigChangeListener listener) {
        this.listener = listener;
    }

    /**
     * Place in the configuration manager where the data about live nodes is stored.
     *
     */
    protected final static String nodeInfoRoot = "/pravega/nodes";
    protected final static String controllerInfoRoot = "/pravega/controllers";

    @Override
    public void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception {
        Map jsonMap = new HashMap<String,Object>();
        jsonMap.put (HOST_STRING, host);
        jsonMap.put (PORT_STRING, port);
        jsonMap.put (METADATA_STRING, jsonMetadata);

        createEntry(nodeInfoRoot + "/" + host + ":" + port, JSONHelper.jsonEncode(jsonMap).getBytes());
    }

    @Override
    public void registerPravegaController(String host, int port, String jsonMetadata) throws Exception {
        Map jsonMap = new HashMap<String,Object>();
        jsonMap.put (HOST_STRING ,host);
        jsonMap.put (PORT_STRING , port);
        jsonMap.put (METADATA_STRING, jsonMetadata);

        createEntry(controllerInfoRoot + "/" + host + ":" + port, JSONHelper.jsonEncode(jsonMap).getBytes());
    }

    @Override
    public void unregisterPravegaController(String host, int port) throws Exception {
        deleteEntry(controllerInfoRoot + "/" + host + ":" + port);
    }

    @Override
    public void unregisterPravegaNode(String host, int port) throws Exception {
        deleteEntry(nodeInfoRoot + "/" + host + ":" + port);

    }

}
