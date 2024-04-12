package org.apache.dubbo.xds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * used to store the instance of HttpFilterConfigFactory
 */
public class HttpFilterConfigFactoryRegistry {
    private static volatile HttpFilterConfigFactoryRegistry instance;

    private final Map<String,HttpFilterConfigFactory> supportedFactories = new ConcurrentHashMap<>();

    public static HttpFilterConfigFactoryRegistry getInstance() {
        if (instance == null) {
            synchronized (HttpFilterConfigFactoryRegistry.class) {
                if (instance == null) {
                    instance = new HttpFilterConfigFactoryRegistry();
                    instance.registerFactory(FaultConfigFactory.getInstance());
                }
            }
        }
        return instance;
    }

    private HttpFilterConfigFactoryRegistry() {
    }

    public void registerFactory(HttpFilterConfigFactory factory) {
        supportedFactories.put(factory.getTypeUrl(), factory);
    }

    public HttpFilterConfigFactory getFactory(String typeUrl) {
        return supportedFactories.get(typeUrl);
    }
}
