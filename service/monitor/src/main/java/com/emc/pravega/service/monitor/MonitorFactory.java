package com.emc.pravega.service.monitor;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MonitorFactory {

    public enum MonitorType {
        ThresholdMonitor
    }

    public static SegmentTrafficMonitor createMonitor(MonitorType monitorType) {
        switch (monitorType) {
            case ThresholdMonitor: {
                return ThresholdMonitor.getMonitor();
            }
            default: {
                throw new NotImplementedException();
            }
        }
    }
}
