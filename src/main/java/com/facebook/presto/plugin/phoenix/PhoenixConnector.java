/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import io.airlift.bootstrap.LifeCycleManager;

import javax.inject.Inject;

import java.util.List;

public class PhoenixConnector
        extends JdbcConnector
{
    private PhoenixTableProperties tableProperties;

    @Inject
    public PhoenixConnector(
            LifeCycleManager lifeCycleManager,
            JdbcMetadataFactory jdbcMetadataFactory,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcRecordSinkProvider jdbcRecordSinkProvider,
            PhoenixTableProperties tableProperties)
    {
        super(lifeCycleManager, jdbcMetadataFactory, jdbcSplitManager, jdbcRecordSetProvider, jdbcRecordSinkProvider);
        this.tableProperties = tableProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }
}
