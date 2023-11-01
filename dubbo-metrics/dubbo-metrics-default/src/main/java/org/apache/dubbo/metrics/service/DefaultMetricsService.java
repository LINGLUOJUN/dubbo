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

package org.apache.dubbo.metrics.service;

import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.metrics.collector.MetricsCollector;
import org.apache.dubbo.metrics.model.MetricsCategory;
import org.apache.dubbo.metrics.model.sample.GaugeMetricSample;
import org.apache.dubbo.metrics.model.sample.MetricSample;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link MetricsService}
 */
public class DefaultMetricsService implements MetricsService {

    @SuppressWarnings("rawtypes")
    protected final List<MetricsCollector> collectors = new ArrayList<>();

    public DefaultMetricsService(ApplicationModel applicationModel) {
        collectors.addAll(applicationModel.getBeanFactory().getBeansOfType(MetricsCollector.class));
    }

    @Override
    public Map<MetricsCategory, List<MetricsEntity>> getMetricsByCategories(List<MetricsCategory> categories) {
        return getMetricsByCategories(null, categories);
    }

    @Override
    public Map<MetricsCategory, List<MetricsEntity>> getMetricsByCategories(String serviceUniqueName, List<MetricsCategory> categories) {
        return getMetricsByCategories(serviceUniqueName, null, null, categories);
    }

    @Override
    public Map<MetricsCategory, List<MetricsEntity>> getMetricsByCategories(String serviceUniqueName, String methodName, Class<?>[] parameterTypes, List<MetricsCategory> categories) {
        return collectors.stream()
            .map(ele -> ((MetricsCollector<?>) ele).collect())
            .filter(CollectionUtils::isNotEmpty)
            .flatMap(Collection::stream)
            .filter(ele -> categories.contains(ele.getCategory()))
            .collect(Collectors.groupingBy(MetricSample::getCategory,
                Collectors.mapping(this::sampleToEntity, Collectors.toList())));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private MetricsEntity sampleToEntity(MetricSample sample) {
        MetricsEntity entity = new MetricsEntity();

        entity.setName(sample.getName());
        entity.setTags(sample.getTags());
        entity.setCategory(sample.getCategory());
        switch (sample.getType()) {
            case GAUGE:
                GaugeMetricSample gaugeSample = (GaugeMetricSample) sample;
                entity.setValue(gaugeSample.getApply().applyAsDouble(gaugeSample.getValue()));
                break;
            case COUNTER:
            case LONG_TASK_TIMER:
            case TIMER:
            case DISTRIBUTION_SUMMARY:
            default:
                break;
        }

        return entity;
    }
}
