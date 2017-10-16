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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableProperty;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static org.apache.hadoop.hbase.HConstants.FOREVER;

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
    @SuppressWarnings("deprecation")
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        Map<String, Object> tableProperties = tableMetadata.getProperties();

        List<String> rowkeys = new ArrayList<>(PhoenixTableProperties.getRowkeys(tableProperties));

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(quoted(catalog, schema, table))
                    .append(" (\n ");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                String typeStatement;

                if (rowkeys.size() == 0) {
                    typeStatement = toSqlType(column.getType()) + " not null";
                    rowkeys.add(columnName);
                }
                else if (rowkeys.stream().anyMatch(columnName::equalsIgnoreCase)) {
                    typeStatement = toSqlType(column.getType()) + " not null";
                }
                else {
                    typeStatement = toSqlType(column.getType());
                }
                columnList.add(new StringBuilder()
                        .append(quoted(columnName))
                        .append(" ")
                        .append(typeStatement)
                        .toString());
            }
            List<String> columns = columnList.build();
            Joiner.on(", \n ").appendTo(sql, columns);
            sql.append("\n CONSTRAINT PK PRIMARY KEY(");
            Joiner.on(", ").appendTo(sql, rowkeys);
            sql.append(")\n)\n");

            ImmutableList.Builder<String> talbeOptions = ImmutableList.builder();
            PhoenixTableProperties.getSaltBuckets(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.SALT_BUCKETS + "=" + value));
            PhoenixTableProperties.getDisableWal(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.DISABLE_WAL + "=" + value));
            PhoenixTableProperties.getImmutableRows(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.IMMUTABLE_ROWS + "=" + value));
            PhoenixTableProperties.getDefaultColumnFamily(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + value));
            PhoenixTableProperties.getBloomfilter(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.BLOOMFILTER + "='" + value + "'"));
            PhoenixTableProperties.getVersions(tableProperties).ifPresent(value -> talbeOptions.add(HConstants.VERSIONS + "=" + value));
            PhoenixTableProperties.getMinVersions(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.MIN_VERSIONS + "=" + value));
            PhoenixTableProperties.getCompression(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.COMPRESSION + "='" + value + "'"));
            PhoenixTableProperties.getTimeToLive(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.TTL + "=" + value));
            Joiner.on(", \n ").appendTo(sql, talbeOptions.build());

            execute(connection, sql.toString());

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    "",
                    connectionUrl,
                    fromProperties(connectionProperties));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
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

    public Map<String, Object> getTableProperties(JdbcTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (PhoenixConnection pconn = driver.connect(connectionUrl, connectionProperties).unwrap(PhoenixConnection.class);
                HBaseAdmin admin = pconn.getQueryServices().getAdmin()) {
            PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName())));

            List<PColumn> pkColumns = table.getPKColumns();
            if (!pkColumns.isEmpty()) {
                properties.put(PhoenixTableProperties.ROWKEYS, table.getPKColumns().stream()
                        .map(PColumn::getName)
                        .map(PName::getString)
                        .filter(name -> !name.startsWith("_"))
                        .collect(Collectors.toList()));
            }

            ImmutableList.Builder<String> talbeOptions = ImmutableList.builder();
            if (table.getBucketNum() != null) {
                properties.put(PhoenixTableProperties.SALT_BUCKETS, table.getBucketNum());
            }
            if (table.isWALDisabled()) {
                properties.put(PhoenixTableProperties.DISABLE_WAL, table.isWALDisabled());
            }
            if (table.isImmutableRows()) {
                properties.put(PhoenixTableProperties.IMMUTABLE_ROWS, table.isImmutableRows());
            }

            String defaultFamilyName = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString();
            if (table.getDefaultFamilyName() != null) {
                properties.put(PhoenixTableProperties.DEFAULT_COLUMN_FAMILY, defaultFamilyName);
            }

            HTableDescriptor tableDesc = admin.getTableDescriptor(table.getPhysicalName().getBytes());

            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (!columnFamily.getBloomFilterType().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.BLOOMFILTER, columnFamily.getBloomFilterType());
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        properties.put(PhoenixTableProperties.VERSIONS, columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        properties.put(PhoenixTableProperties.MIN_VERSIONS, columnFamily.getMinVersions());
                    }
                    if (!columnFamily.getCompression().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.COMPRESSION, columnFamily.getCompression());
                    }
                    if (columnFamily.getTimeToLive() < FOREVER) {
                        properties.put(PhoenixTableProperties.TTL, columnFamily.getTimeToLive());
                    }
                    break;
                }
            }
            List<String> options = talbeOptions.build();
            if (options.size() > 0) {
                StringBuilder tableOptions = new StringBuilder();
                Joiner.on(", \n ").appendTo(tableOptions, options);
            }
        }
        catch (IOException | SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return properties.build();
    }
}
