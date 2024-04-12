package org.apache.dubbo.xds.resource;

import com.google.protobuf.Any;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.envoyproxy.envoy.extensions.filters.common.fault.v3.FaultDelay;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.FaultAbort;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.HTTPFault;
import io.envoyproxy.envoy.type.v3.FractionalPercent;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_ERROR_RESPONSE_XDS;

/**
 * configuration for fault injection.
 * see <a href="https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/http/fault/v3/fault.proto#extensions-filters-http-fault-v3-httpfault">HTTPFault</a>
 */
public class XdsFaultConfig implements XdsHttpFilterConfig {

    private final XdsFaultDelay xdsFaultDelay;

    private final XdsFaultAbort xdsFaultAbort;

    private final Integer maxActiveFaults;

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(XdsFaultConfig.class);

    public static final String TYPE_URL = "type.googleapis.com/envoy.extensions.filters.http.fault.v3.HTTPFault";

    public XdsFaultConfig(XdsFaultDelay xdsFaultDelay, XdsFaultAbort xdsFaultAbort, Integer maxActiveFaults) {
        this.xdsFaultDelay = xdsFaultDelay;
        this.xdsFaultAbort = xdsFaultAbort;
        this.maxActiveFaults = maxActiveFaults;
    }

    public XdsFaultDelay getXdsFaultDelay() {
        return xdsFaultDelay;
    }

    public XdsFaultAbort getXdsFaultAbort() {
        return xdsFaultAbort;
    }

    public Integer getMaxActiveFaults() {
        return maxActiveFaults;
    }

    @Override
    public String typeUrl() {
        return TYPE_URL;
    }

    /**
     * fault injection configuration for delaying requests
     */
    public static class XdsFaultDelay {
        private final Long delayNanos;
        private final boolean headerDelay;
        private final XdsFractionalPercent percent;

        public XdsFaultDelay(Long delayNanos, boolean headerDelay, XdsFractionalPercent percent) {
            this.delayNanos = delayNanos;
            this.headerDelay = headerDelay;
            this.percent = percent;
        }

        public static XdsFaultDelay create(Long delayNanos, boolean headerDelay, XdsFractionalPercent percent) {
            return new XdsFaultDelay(delayNanos, headerDelay, percent);
        }

        public Long getDelayNanos() {
            return delayNanos;
        }

        public boolean isHeaderDelay() {
            return headerDelay;
        }

        public XdsFractionalPercent getPercent() {
            return percent;
        }
    }

    /**
     * fault injection configuration for aborting requests
     */
    public static class XdsFaultAbort {
        private final int httpStatus;
        private final int grpcStatus;
        private final boolean headerAbort;
        private final XdsFractionalPercent percent;

        public XdsFaultAbort(int httpStatus, int grpcStatus, boolean headerAbort, XdsFractionalPercent percent) {
            this.httpStatus = httpStatus;
            this.grpcStatus = grpcStatus;
            this.headerAbort = headerAbort;
            this.percent = percent;
        }

        public static XdsFaultAbort create(int httpStatus, int grpcStatus, boolean headerAbort, XdsFractionalPercent percent) {
            return new XdsFaultAbort(httpStatus, grpcStatus, headerAbort, percent);
        }

        public int getHttpStatus() {
            return httpStatus;
        }

        public int getGrpcStatus() {
            return grpcStatus;
        }

        public boolean isHeaderAbort() {
            return headerAbort;
        }
    }

    public static class XdsFractionalPercent {
        private final int numerator;
        private final DenominatorType denominatorType;

        public XdsFractionalPercent(int numerator, DenominatorType denominatorType) {
            this.numerator = numerator;
            this.denominatorType = denominatorType;
        }

        public static XdsFractionalPercent create(int numerator, DenominatorType denominatorType) {
            return new XdsFractionalPercent(numerator, denominatorType);
        }
    }

    public enum DenominatorType {
        HUNDRED, TEN_THOUSAND, MILLION
    }

    @Override
    public String toString() {
        return "XdsFaultConfig{" + "xdsFaultDelay=" + xdsFaultDelay.delayNanos + ", xdsFaultAbort=" + xdsFaultAbort.httpStatus + '}';
    }
}
