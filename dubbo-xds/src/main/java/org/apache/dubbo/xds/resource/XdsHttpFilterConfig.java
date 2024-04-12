package org.apache.dubbo.xds.resource;


import com.google.protobuf.Any;

/**
 * defines the Http filter config.
 */
public interface XdsHttpFilterConfig {
    String typeUrl();
    /**
     * top-level configuration of the httpFilter filter, contains the filter name and the filter configuration
     */
    final class TopFilterConfig{
        final String name;
        final XdsHttpFilterConfig filterConfig;

        TopFilterConfig(String name, XdsHttpFilterConfig filterConfig) {
            this.name = name;
            this.filterConfig = filterConfig;
        }
    }

}
