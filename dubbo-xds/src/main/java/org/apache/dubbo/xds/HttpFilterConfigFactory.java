package org.apache.dubbo.xds;

import com.google.protobuf.Any;
import org.apache.dubbo.xds.resource.XdsHttpFilterConfig;


/**
 * used to create filter config based on the proto
 */
public interface HttpFilterConfigFactory {

    public XdsHttpFilterConfig parseHttpFilterConfig(Any proto);

    public String getTypeUrl();

}
