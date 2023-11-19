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

package org.apache.dubbo.metrics.collector.sample;

import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.metrics.collector.DefaultMetricsCollector;
import org.apache.dubbo.metrics.model.MetricsCategory;
import org.apache.dubbo.metrics.model.ThreadPoolRejectMetric;
import org.apache.dubbo.metrics.model.key.MetricsKey;
import org.apache.dubbo.metrics.model.sample.GaugeMetricSample;
import org.apache.dubbo.metrics.model.sample.MetricSample;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static org.apache.dubbo.metrics.model.MetricsCategory.THREAD_POOL;

public class ThreadRejectMetricsCountSampler extends SimpleMetricsCountSampler<String, String, ThreadPoolRejectMetric> {

    private final DefaultMetricsCollector collector;

    private final Set<String> metricNames = new ConcurrentHashSet<>();
    private final AtomicBoolean samplesChanged = new AtomicBoolean(true);

    public ThreadRejectMetricsCountSampler(DefaultMetricsCollector collector) {
        this.collector = collector;
        this.collector.addSampler(this);
    }

    public void addMetricName(String name) {
        this.metricNames.add(name);
        this.initMetricsCounter(name, name);
        samplesChanged.set(true);
    }

    @Override
    public List<MetricSample> sample() {
        return metricNames
            .stream()
            .map(this::convertThreadRejectMetric2GaugeMetricSample)
            .filter(CollectionUtils::isNotEmpty)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }


    private List<MetricSample> convertThreadRejectMetric2GaugeMetricSample(String metricName) {
        return getCount(metricName).entrySet()
            .stream()
            .map(ele -> getGaugeMetricSample(MetricsKey.THREAD_POOL_THREAD_REJECT_COUNT, ele.getKey(), THREAD_POOL, ele.getValue(), AtomicLong::get))
            .collect(Collectors.toList());

    }

    private <T> GaugeMetricSample<T> getGaugeMetricSample(MetricsKey metricsKey,
                                                          ThreadPoolRejectMetric methodMetric,
                                                          MetricsCategory metricsCategory,
                                                          T value,
                                                          ToDoubleFunction<T> apply) {
        return new GaugeMetricSample<>(
            metricsKey.getNameByType(methodMetric.getThreadPoolName()),
            metricsKey.getDescription(),
            methodMetric.getTags(),
            metricsCategory,
            value,
            apply);
    }





    @Override
    protected void countConfigure(MetricsCountSampleConfigurer<String, String, ThreadPoolRejectMetric> sampleConfigure) {
        sampleConfigure.configureMetrics(configure -> new ThreadPoolRejectMetric(collector.getApplicationName(), configure.getSource()));
    }

    @Override
    public boolean calSamplesChanged() {
        // CAS to get and reset the flag in an atomic operation
        return samplesChanged.compareAndSet(true, false);
    }
}
