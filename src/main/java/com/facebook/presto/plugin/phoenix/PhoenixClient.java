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

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.phoenix.jdbc.PhoenixDriver;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Collections.nCopies;

public class PhoenixClient
        extends BaseJdbcClient
{
    @Inject
    public PhoenixClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException
    {
        super(connectorId, config, "", new PhoenixDriver());
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle)
    {
        JdbcTableHandle tableHandle = layoutHandle.getTable();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                connectionUrl,
                fromProperties(connectionProperties),
                layoutHandle.getTupleDomain());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE not yet supported for Phoenix");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorTableMetadata tableMetadata)
    {
        return new JdbcOutputTableHandle(
                connectorId,
                "",
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getType).collect(Collectors.toList()),
                "",
                connectionUrl,
                fromProperties(connectionProperties));
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
    }

    @Override
    public void finishInsertTable(JdbcOutputTableHandle handle)
    {
    }

    @Override
    public void rollbackCreateTable(JdbcOutputTableHandle handle)
    {
        dropTable(new JdbcTableHandle(
                handle.getConnectorId(),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTableName()));
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        String vars = Joiner.on(',').join(nCopies(handle.getColumnNames().size(), "?"));
        return new StringBuilder()
                .append("UPSERT INTO ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()))
                .append(" VALUES (").append(vars).append(")")
                .toString();
    }

    @Override
    protected String toSqlType(Type type)
    {
        String sqlType = super.toSqlType(type);
        switch (sqlType) {
            case "double precision":
                return "double";
            case "real":
                return "float";
            case "time with timezone":
                return "time";
            case "timestamp with timezone":
                return "timestamp";
        }
        return sqlType;
    }
}
