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
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.phoenix.PhoenixTableProperties.ROWKEYS;
import static com.facebook.presto.plugin.phoenix.PhoenixTableProperties.TABLE_OPTIONS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.collect.Maps.fromProperties;
import static org.apache.hadoop.hbase.HConstants.FOREVER;

import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;

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
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        Map<String, Object> tableProperties = tableMetadata.getProperties();
        List<String> rowkeys = PhoenixTableProperties.getRowkeys(tableProperties).orElse(emptyList());
        Optional<String> tableOptions = PhoenixTableProperties.getTableOptions(tableProperties);

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
            List<String> pkColumnList = new ArrayList<>();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                String typeStatement;

                boolean isPkColumn = (rowkeys.size() == 0 && pkColumnList.size() == 0) ||
                        rowkeys.stream().anyMatch(columnName::equalsIgnoreCase);
                if (isPkColumn) {
                    typeStatement = toSqlType(column.getType()) + " not null";
                    pkColumnList.add(columnName);
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
            Joiner.on(", ").appendTo(sql, pkColumnList);
            sql.append(")\n)\n");

            if (tableOptions.isPresent()) {
                final Matcher matcher = Pattern.compile("\\bforever\\b", Pattern.CASE_INSENSITIVE).matcher(tableOptions.get());
                sql.append(matcher.replaceAll(Integer.toString(FOREVER)).replace('"', '\''));
            }

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

    @SuppressWarnings("deprecation")
    public Map<String, Object> getTableProperties(JdbcTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (PhoenixConnection pconn = driver.connect(connectionUrl, connectionProperties).unwrap(PhoenixConnection.class);
                HBaseAdmin admin = pconn.getQueryServices().getAdmin()) {
            PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName())));

            properties.put(ROWKEYS, table.getPKColumns().stream()
                    .map(PColumn::getName)
                    .map(PName::getString)
                    .filter(name -> !name.startsWith("_"))
                    .collect(Collectors.joining(", ")));

            ImmutableList.Builder<String> talbeOptions = ImmutableList.builder();
            if (table.getBucketNum() != null) {
                talbeOptions.add(TableProperty.SALT_BUCKETS + "=" + table.getBucketNum());
            }
            if (table.isWALDisabled()) {
                talbeOptions.add(TableProperty.DISABLE_WAL + "=" + table.isWALDisabled());
            }
            if (table.isImmutableRows()) {
                talbeOptions.add(TableProperty.IMMUTABLE_ROWS + "=" + table.isImmutableRows());
            }

            String defaultFamilyName = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString();
            if (table.getDefaultFamilyName() != null) {
                talbeOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + defaultFamilyName);
            }
            if (table.getStoreNulls()) {
                talbeOptions.add(TableProperty.STORE_NULLS + "=" + table.getStoreNulls());
            }

            HTableDescriptor tableDesc = admin.getTableDescriptor(table.getPhysicalName().getBytes());

            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (!columnFamily.getBloomFilterType().toString().equals("NONE")) {
                        talbeOptions.add(HColumnDescriptor.BLOOMFILTER + "=\"" + columnFamily.getBloomFilterType() + "\"");
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        talbeOptions.add(HConstants.VERSIONS + "=" + columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        talbeOptions.add(HColumnDescriptor.MIN_VERSIONS + "=" + columnFamily.getMinVersions());
                    }
                    if (!columnFamily.getCompression().toString().equals("NONE")) {
                        talbeOptions.add(HColumnDescriptor.COMPRESSION + "=\"" + columnFamily.getCompression() + "\"");
                    }
                    if (columnFamily.getTimeToLive() < FOREVER) {
                        talbeOptions.add(HColumnDescriptor.TTL + "=" + columnFamily.getTimeToLive());
                    }
                    break;
                }
            }
            List<String> options = talbeOptions.build();
            if (options.size() > 0) {
                StringBuilder tableOptions = new StringBuilder();
                Joiner.on(", \n ").appendTo(tableOptions, options);
                properties.put(TABLE_OPTIONS, tableOptions.toString());
            }
        }
        catch (IOException | SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return properties.build();
    }
}
