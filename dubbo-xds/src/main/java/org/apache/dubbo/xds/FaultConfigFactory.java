package org.apache.dubbo.xds;

import com.google.protobuf.Any;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.envoyproxy.envoy.extensions.filters.common.fault.v3.FaultDelay;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.FaultAbort;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.HTTPFault;
import io.envoyproxy.envoy.type.v3.FractionalPercent;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.xds.resource.XdsFaultConfig;
import org.apache.dubbo.xds.resource.XdsFaultConfig.XdsFaultAbort;
import org.apache.dubbo.xds.resource.XdsFaultConfig.XdsFaultDelay;
import org.apache.dubbo.xds.resource.XdsFaultConfig.XdsFractionalPercent;


import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_ERROR_RESPONSE_XDS;

public class FaultConfigFactory implements HttpFilterConfigFactory{

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(FaultConfigFactory.class);

    private static volatile FaultConfigFactory instance;

    public static FaultConfigFactory getInstance() {
        if (instance == null) {
            synchronized (FaultConfigFactory.class) {
                if (instance == null) {
                    instance = new FaultConfigFactory();
                }
            }
        }
        return instance;
    }

    @Override
    public XdsFaultConfig parseHttpFilterConfig(Any proto){
        HTTPFault httpFault = unpackHTTPFault(proto);
        if(httpFault == null){
            return null;
        }
        XdsFaultDelay delay = null;
        XdsFaultAbort abort = null;
        if(httpFault.hasDelay()){
            delay = parseFaultDelay(httpFault.getDelay());
        }
        if(httpFault.hasAbort()){
            abort = parseFaultAbort(httpFault.getAbort());
        }
        Integer maxActiveFaults = null;
        if(httpFault.hasMaxActiveFaults()){
            maxActiveFaults = httpFault.getMaxActiveFaults().getValue();
            if(maxActiveFaults < 0){
                maxActiveFaults = Integer.MAX_VALUE;
            }
        }
        return new XdsFaultConfig(delay, abort, maxActiveFaults);
    }

    @Override
    public String getTypeUrl() {
        return XdsFaultConfig.TYPE_URL;
    }

    private static HTTPFault unpackHTTPFault(Any any) {
        try {
            return any.unpack(HTTPFault.class);
        } catch (InvalidProtocolBufferException e) {
            logger.error(REGISTRY_ERROR_RESPONSE_XDS, "", "", "Error occur when decode xDS response.", e);
            return null;
        }
    }

    public XdsFaultDelay parseFaultDelay(FaultDelay faultDelay) {
        XdsFractionalPercent percent = parseFractionalPercent(faultDelay.getPercentage());
        if(faultDelay.hasHeaderDelay()){
            return XdsFaultDelay.create(null, true, percent);
        }
        return XdsFaultDelay.create(Durations.toNanos(faultDelay.getFixedDelay()), false, percent);
    }

    public XdsFaultAbort parseFaultAbort(FaultAbort faultAbort) {
        XdsFractionalPercent percent = parseFractionalPercent(faultAbort.getPercentage());
        switch (faultAbort.getErrorTypeCase()) {
            case HTTP_STATUS:
                return XdsFaultAbort.create(faultAbort.getHttpStatus(), 0, false, percent);
            case GRPC_STATUS:
                return XdsFaultAbort.create(0, faultAbort.getGrpcStatus(), false, percent);
            case HEADER_ABORT:
                return XdsFaultAbort.create(0, 0, true, percent);
            default:
                logger.error(REGISTRY_ERROR_RESPONSE_XDS, "", "", "Error occur when decode xDS response.");
                return null;
        }
    }

    public XdsFractionalPercent parseFractionalPercent(FractionalPercent percent){
        switch (percent.getDenominator()) {
            case HUNDRED:
                return XdsFractionalPercent.create(percent.getNumerator(), XdsFractionalPercent.DenominatorType.HUNDRED);
            case TEN_THOUSAND:
                return XdsFractionalPercent.create(percent.getNumerator(), XdsFractionalPercent.DenominatorType.TEN_THOUSAND);
            case MILLION:
                return XdsFractionalPercent.create(percent.getNumerator(), XdsFractionalPercent.DenominatorType.MILLION);
            default:
                logger.error(REGISTRY_ERROR_RESPONSE_XDS, "", "", "Error occur when decode xDS response.");
                return null;
        }
    }
}
