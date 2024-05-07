/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.xds.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.FailbackClusterInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;
import org.apache.dubbo.xds.resource.XdsFaultConfig;
import org.apache.dubbo.xds.resource.XdsFaultConfig.XdsFaultAbort;
import org.apache.dubbo.xds.resource.XdsFaultConfig.XdsFaultDelay;
import org.apache.dubbo.xds.resource.XdsFaultConfig.XdsFractionalPercent;
import org.apache.dubbo.xds.resource.XdsHttpFilterConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class XdsClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private volatile Timer delayTimer;
    private final AtomicLong activeFaults;
    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(XdsClusterInvoker.class);

    public XdsClusterInvoker(Directory<T> directory) {
        super(directory);
        activeFaults = new AtomicLong(0);
    }

    @Override
    protected Result doInvoke(
            Invocation invocation,
            List<Invoker<T>> invokers,
            LoadBalance loadbalance) throws RpcException {
        while (true) {
            Invoker<T> invoker = select(loadbalance, invocation, invokers, null);
            //TODO: optimize the way to get httpFilterConfigs
            Map<String, XdsHttpFilterConfig> httpFilterConfigs =
                    (Map<String, XdsHttpFilterConfig>) invocation.getObjectAttachment("httpFilterConfig");

            if (httpFilterConfigs != null) {
                invocation.getObjectAttachments()
                        .remove("httpFilterConfig");
            }
            try {
                if (httpFilterConfigs != null) {
                    XdsFaultConfig faultConfig = (XdsFaultConfig) httpFilterConfigs.get(XdsFaultConfig.TYPE_URL);
                    if (faultConfig != null) {
                        if (injectActiveFault(faultConfig, invocation, invokers, loadbalance, invoker.getUrl())) {
                            logger.info("start fault injection");
                            return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation); // ignore
                        }
                    }
                }
                return invokeWithContext(invoker, invocation);
            } catch (Throwable e) {
                if (e instanceof RpcException && ((RpcException) e).isBiz()) { // biz exception.
                    throw (RpcException) e;
                }
                throw new RpcException(e instanceof RpcException ? ((RpcException) e).getCode() : 0,
                        "Xds invoke providers " + invoker.getUrl() + " " + loadbalance.getClass()
                                .getSimpleName() + " for service " + getInterface().getName() + " method "
                                + RpcUtils.getMethodName(invocation) + " on consumer " + NetUtils.getLocalHost()
                                + " use dubbo version " + Version.getVersion()
                                + ", but no luck to perform the invocation. Last error is: " + e.getMessage(),
                        e.getCause() != null ? e.getCause() : e);
            }
        }
    }

    /**
     * Add an active fault to the invoker
     *
     * @return true if the fault is injected successfully, false otherwise
     */
    private boolean injectActiveFault(
            XdsFaultConfig faultConfig,
            Invocation invocation,
            List<Invoker<T>> invokers,
            LoadBalance loadbalance,
            URL consumerUrl) {
        Long delayNanos = null;
        Integer httpStatus = null;
        Integer grpcStatus = null;

        if (delayTimer == null) {
            synchronized (this) {
                if (delayTimer == null) {
                    delayTimer = new HashedWheelTimer(new NamedThreadFactory("xds-cluster-timer", true), 100,
                            TimeUnit.MILLISECONDS, 128);
                }
            }
        }

        //if the number of active faults exceeds the maxActiveFaults, return false
        if (faultConfig.getMaxActiveFaults() != null && activeFaults.get() >= faultConfig.getMaxActiveFaults()) {
            return false;
        }

        XdsFaultDelay delay = faultConfig.getXdsFaultDelay();
        if (delay != null) {
            XdsFractionalPercent percent = delay.getPercent();
            if(ThreadLocalRandom.current().nextInt(1000000) < getRatePerMillion(percent)) {
                delayNanos = delay.getDelayNanos();
            }
        }

        XdsFaultAbort abort = faultConfig.getXdsFaultAbort();
        if (abort != null) {
            XdsFractionalPercent percent = delay.getPercent();
            if(ThreadLocalRandom.current().nextInt(1000000) < getRatePerMillion(percent)) {
                httpStatus = abort.getHttpStatus();
                grpcStatus = abort.getGrpcStatus();
            }
        }

        //if no fault injection is needed, return false
        if (delayNanos == null && httpStatus == null && grpcStatus == null) {
            return false;
        }

        if (delayNanos != null) {
            DelayTimerTask retryTimerTask;
            if (httpStatus != null || grpcStatus != null) {
                retryTimerTask = new DelayTimerTask(invocation, invokers, loadbalance, httpStatus, grpcStatus, true);
            } else {
                retryTimerTask = new DelayTimerTask(invocation, invokers, loadbalance, null, null, false);
            }
            delayTimer.newTimeout(retryTimerTask, delayNanos, TimeUnit.NANOSECONDS);
        } else if (httpStatus != null || grpcStatus != null) {
            throw new RpcException(
                    httpStatus == null ? grpcStatus : httpStatus,
                    "AbortFault Injection " + " for service " + getInterface().getName() + " method "
                            + RpcUtils.getMethodName(invocation) + " on consumer " + NetUtils.getLocalHost()
                            + " use dubbo version " + Version.getVersion() + "with httpStatus " + httpStatus
                            + "and grpcStatus " + grpcStatus);
        }
        return true;
    }

    /**
     * delay injection
     */
    private class DelayTimerTask implements TimerTask {
        private final Invocation invocation;
        private final List<Invoker<T>> invokers;
        private final LoadBalance loadbalance;

        private final Integer httpStatus;

        private final Integer grpcStatus;

        private final Boolean isAbort;

        public DelayTimerTask(
                Invocation invocation,
                List<Invoker<T>> invokers,
                LoadBalance loadbalance,
                Integer httpStatus,
                Integer grpcStatus,
                Boolean isAbort) {
            this.invocation = invocation;
            this.invokers = invokers;
            this.loadbalance = loadbalance;
            this.httpStatus = httpStatus;
            this.grpcStatus = grpcStatus;
            this.isAbort = isAbort;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            try {
                activeFaults.incrementAndGet();
                if (isAbort) {
                    //abort the request,throw exception
                    throw new RpcException(
                            httpStatus == null ? grpcStatus : httpStatus,
                            "AbortFault Injection " + " for service " + getInterface().getName() + " method "
                                    + RpcUtils.getMethodName(invocation) + " on consumer " + NetUtils.getLocalHost()
                                    + " use dubbo version " + Version.getVersion() + "with httpStatus " + httpStatus
                                    + "and grpcStatus " + grpcStatus);
                } else {
                    logger.info("Send the request after delay injection");
                    Invoker<T> invoker = select(loadbalance, invocation, invokers, null);
                    invokeWithContext(invoker, invocation);
                }
            } finally {
                activeFaults.decrementAndGet();
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    /**
     * see <a href="https://www.envoyproxy.io/docs/envoy/latest/api-v3/type/v3/percent.proto#type-v3-fractionalpercent">FractionalPercent</a>
     */
    private static int getRatePerMillion(XdsFractionalPercent percent) {
        int numerator = percent.getNumerator();
        XdsFractionalPercent.DenominatorType type = percent.getDenominatorType();
        switch (type) {
            case HUNDRED:
                numerator *= 10000;
                break;
            case TEN_THOUSAND:
                numerator *= 100;
                break;
            case MILLION:
            default:
                break;
        }
        if(numerator > 1000000 || numerator < 0){
            return 1000000;
        }
        return numerator;
    }
}
