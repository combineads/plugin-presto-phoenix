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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

import static java.util.Objects.requireNonNull;

public class PhoenixMetadata
        extends JdbcMetadata
{
    private final JdbcClient jdbcClient;

    public PhoenixMetadata(JdbcClient jdbcClient, boolean allowDropTable)
    {
        super(jdbcClient, allowDropTable);
        this.jdbcClient = requireNonNull(jdbcClient, "client is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (JdbcColumnHandle column : jdbcClient.getColumns(handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build(), ((PhoenixClient) jdbcClient).getTableProperties(handle));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        jdbcClient.beginCreateTable(tableMetadata);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }
}
