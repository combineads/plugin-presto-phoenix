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

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;

import static java.util.Objects.requireNonNull;

public class PhoenixClientModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public PhoenixClientModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSinkProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);

        binder.bind(JdbcClient.class).to(PhoenixClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcMetadataFactory.class).to(PhoenixMetadataFactory.class).in(Scopes.SINGLETON);

        binder.bind(PhoenixTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(PhoenixConnector.class).in(Scopes.SINGLETON);

        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            PhoenixDriver driver = PhoenixDriver.INSTANCE;
            checkArgument(driver.acceptsURL(connectionUrl), "Invalid JDBC URL for Phoenix connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
