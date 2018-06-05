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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RoundRobinResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.types.PDataType;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static com.facebook.presto.plugin.phoenix.TypeUtils.isArrayType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hbase.HConstants.FOREVER;
import static org.apache.phoenix.util.PhoenixRuntime.getTable;

public class PhoenixClient
{
    public static final String ROWKEY = "ROWKEY";
    private static final Logger log = Logger.get(PhoenixClient.class);
    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "BOOLEAN")
            .put(BIGINT, "BIGINT")
            .put(INTEGER, "INTEGER")
            .put(SMALLINT, "SMALLINT")
            .put(TINYINT, "TINYINT")
            .put(DOUBLE, "DOUBLE")
            .put(REAL, "FLOAT")
            .put(VARBINARY, "VARBINARY")
            .put(DATE, "DATE")
            .put(TIME, "TIME")
            .put(TIME_WITH_TIME_ZONE, "TIME")
            .put(TIMESTAMP, "TIMESTAMP")
            .put(TIMESTAMP_WITH_TIME_ZONE, "TIMESTAMP")
            .build();

    private final String connectorId;
    private final Driver driver = new PhoenixDriver();
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final TypeManager typeManager;

    private final Map<String, HostAddress> hostCache = new HashMap<>();

    @Inject
    public PhoenixClient(PhoenixConnectorId connectorId, PhoenixConfig config, TypeManager typeManager) throws SQLException
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(config, "config is null");
        connectionUrl = config.getConnectionUrl();
        connectionProperties = new Properties();
        connectionProperties.putAll(config.getConnectionProperties());

        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Set<String> getSchemaNames()
    {
        try (PhoenixConnection connection = getConnection();
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("system")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public List<SchemaTableName> getTableNames(@Nullable String schema)
    {
        try (PhoenixConnection connection = getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
                schema = schema.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Nullable
    public PhoenixTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = TableUtils.normalizeTableName(schemaTableName.getTableName());

        try (PhoenixConnection connection = getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                schemaName = schemaName.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schemaName, tableName)) {
                List<PhoenixTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new PhoenixTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public List<PhoenixColumnHandle> getDynamicColumns(String tableName)
    {
        return TableUtils.getDynamicColumnHandles(connectorId, typeManager, tableName);
    }

    public List<PhoenixColumnHandle> getColumns(PhoenixTableHandle tableHandle, boolean reqiuredRowKey)
    {
        try (PhoenixConnection connection = getConnection()) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<PhoenixColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"), resultSet.getInt("DECIMAL_DIGITS"), resultSet.getInt("ARRAY_SIZE"), resultSet.getInt("TYPE_ID"));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        if (reqiuredRowKey || !ROWKEY.equals(columnName)) {
                            columns.add(new PhoenixColumnHandle(connectorId, columnName, columnType));
                        }
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, "Table has no supported column types: " + tableHandle.getSchemaTableName());
                }
                for (PhoenixColumnHandle phoenixColumnHandle : getDynamicColumns(tableHandle.getSchemaTableName().getTableName())) {
                    columns.add(phoenixColumnHandle);
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public ConnectorSplitSource getSplits(PhoenixTableLayoutHandle layoutHandle)
    {
        PhoenixTableHandle handle = layoutHandle.getTable();
        SchemaTableName schemaTableName = handle.getSchemaTableName();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        try (PhoenixConnection connection = getConnection()) {
            String inputQuery = buildSql(connection,
                    handle.getCatalogName(),
                    schemaName,
                    tableName,
                    layoutHandle.getTupleDomain(),
                    getColumns(handle, false));

            final QueryPlan queryPlan = getQueryPlan(connection, inputQuery);
            final List<KeyRange> splits = new LinkedList<>();

            for (List<Scan> scans : queryPlan.getScans()) {
                splits.add(KeyRange.getKeyRange(scans.get(0).getStartRow(), scans.get(scans.size() - 1).getStopRow()));
            }

            byte[] hbaseTableName = queryPlan.getTableRef().getTable().getPhysicalName().getBytes();
            return new FixedSplitSource(splits.stream().map(split -> (KeyRange) split).map(split -> {
                List<HostAddress> addresses;
                try {
                    HRegionLocation location = connection.getQueryServices().getTableRegionLocation(hbaseTableName, split.getLowerRange());
                    String hostName = location.getHostname();
                    HostAddress address = hostCache.get(hostName);
                    if (address == null) {
                        address = HostAddress.fromString(hostName);
                        hostCache.put(hostName, address);
                    }
                    addresses = ImmutableList.of(address);
                }
                catch (SQLException e) {
                    addresses = ImmutableList.of();
                }

                return new PhoenixSplit(
                        connectorId,
                        handle.getCatalogName(),
                        schemaName,
                        schemaTableName.getTableName(),
                        layoutHandle.getTupleDomain(),
                        split,
                        addresses);
            }).collect(Collectors.toList()));
        }
        catch (IOException | InterruptedException | SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public PhoenixResultSet getResultSet(String inputQuery, KeyRange inputSplitKeyRange) throws Exception
    {
        List<Scan> inputSplitScans = null;
        QueryPlan queryPlan = null;

        try (PhoenixConnection connection = getConnection()) {
            queryPlan = getQueryPlan(connection, inputQuery);
            for (List<Scan> scans : queryPlan.getScans()) {
                if (KeyRange.getKeyRange(scans.get(0).getStartRow(), scans.get(scans.size() - 1).getStopRow()).equals(inputSplitKeyRange)) {
                    inputSplitScans = scans;
                    break;
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        inputSplitScans = requireNonNull(inputSplitScans, "inputSplitScans is null");

        List<PeekingResultIterator> iterators = Lists.newArrayListWithExpectedSize(inputSplitScans.size());
        for (Scan scan : inputSplitScans) {
            // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
            scan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

            PeekingResultIterator peekingResultIterator;
            final TableResultIterator tableResultIterator = new TableResultIterator(
                    queryPlan.getContext().getConnection().getMutationState(),
                    scan,
                    null,
                    queryPlan.getContext().getConnection().getQueryServices().getRenewLeaseThresholdMilliSeconds(),
                    queryPlan,
                    MapReduceParallelScanGrouper.getInstance());
            peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);

            iterators.add(peekingResultIterator);
        }
        ResultIterator iterator = queryPlan.useRoundRobinIterator() ? RoundRobinResultIterator.newIterator(iterators, queryPlan) : ConcatResultIterator.newIterator(iterators);
        if (queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) {
            iterator = new SequenceResultIterator(iterator, queryPlan.getContext().getSequenceManager());
        }
        // Clone the row projector as it's not thread safe and would be used simultaneously by
        // multiple threads otherwise.

        return new PhoenixResultSet(iterator, queryPlan.getProjector()
                .cloneIfNecessary(),
                queryPlan.getContext());
    }

    public PhoenixConnection getConnection()
            throws SQLException
    {
        return driver.connect(connectionUrl, connectionProperties).unwrap(PhoenixConnection.class);
    }

    public String buildSql(PhoenixConnection connection,
            String catalogName,
            String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> tupleDomain,
            List<PhoenixColumnHandle> columnHandles)
            throws SQLException, IOException, InterruptedException
    {
        String phoenixTableName = TableUtils.normalizeTableName(tableName);
        List<PhoenixColumnHandle> dynamicColumnHandlers = getDynamicColumns(tableName);
        if (dynamicColumnHandlers.size() > 0) {
            List<String> dynamicColumns = dynamicColumnHandlers.stream().map(column -> new StringBuffer(column.getColumnName()).append(" ").append(toSqlType(column.getColumnType())).toString()).collect(Collectors.toList());
            phoenixTableName += "(" + Joiner.on(',').join(dynamicColumns) + ")";
        }

        return new QueryBuilder().buildSql(
                connection,
                catalogName,
                schemaName,
                phoenixTableName,
                columnHandles,
                tupleDomain);
    }

    public QueryPlan getQueryPlan(PhoenixConnection connection, String inputQuery)
    {
        requireNonNull(inputQuery, "inputQuery is null");
        try (Statement statement = connection.createStatement()) {
            final PhoenixStatement phoenixStmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = phoenixStmt.optimizeQuery(inputQuery);
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        }
        catch (Exception e) {
            throw new PrestoException(PHOENIX_ERROR, String.format("Failed to get the query plan with error [%s]", e.getMessage()), e);
        }
    }

    @SuppressWarnings("deprecation")
    public PhoenixOutputTableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        LinkedList<ColumnMetadata> tableColumns = new LinkedList<>(tableMetadata.getColumns());
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        Map<String, Object> tableProperties = tableMetadata.getProperties();

        List<String> rowkeys = new ArrayList<>(PhoenixTableProperties.getRowkeys(tableProperties));
        List<String> pkColumns = rowkeys.stream().map(name -> name.split("\\s")[0]).collect(Collectors.toList());

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        if (rowkeys.size() == 0) {
            // Add the rowkey when rowkey is empty.
            tableColumns.addFirst(new ColumnMetadata(ROWKEY, VARCHAR));
        }

        try (PhoenixConnection connection = getConnection()) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(getFullTableName(catalog, schema, table))
                    .append(" (\n ");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();

            for (ColumnMetadata column : tableColumns) {
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
                else if (pkColumns.stream().anyMatch(columnName::equalsIgnoreCase)) {
                    typeStatement = toSqlType(column.getType()) + " not null";
                }
                else {
                    typeStatement = toSqlType(column.getType());
                }
                columnList.add(new StringBuilder()
                        .append(columnName)
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
            PhoenixTableProperties.getSplitOn(tableProperties).ifPresent(value -> talbeOptions.add("SPLIT ON (" + value.replace('"', '\'') + ")"));
            PhoenixTableProperties.getDisableWal(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.DISABLE_WAL + "=" + value));
            PhoenixTableProperties.getImmutableRows(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.IMMUTABLE_ROWS + "=" + value));
            PhoenixTableProperties.getDefaultColumnFamily(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + value));
            PhoenixTableProperties.getUpdateCacheFrequency(tableProperties).ifPresent(value -> talbeOptions.add(TableProperty.UPDATE_CACHE_FREQUENCY + "=" + value));
            PhoenixTableProperties.getBloomfilter(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.BLOOMFILTER + "='" + value + "'"));
            PhoenixTableProperties.getVersions(tableProperties).ifPresent(value -> talbeOptions.add(HConstants.VERSIONS + "=" + value));
            PhoenixTableProperties.getMinVersions(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.MIN_VERSIONS + "=" + value));
            PhoenixTableProperties.getCompression(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.COMPRESSION + "='" + value + "'"));
            PhoenixTableProperties.getTimeToLive(tableProperties).ifPresent(value -> talbeOptions.add(HColumnDescriptor.TTL + "=" + value));
            Joiner.on(", \n ").appendTo(sql, talbeOptions.build());

            execute(connection, sql.toString());

            return new PhoenixOutputTableHandle(
                    connectorId,
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build());
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public PhoenixOutputTableHandle beginInsertTable(ConnectorTableMetadata tableMetadata)
    {
        return new PhoenixOutputTableHandle(
                connectorId,
                "",
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getType).collect(Collectors.toList()));
    }

    public void dropTable(PhoenixTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (PhoenixConnection connection = getConnection()) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public void addColumn(PhoenixTableHandle handle, ColumnMetadata column)
    {
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()))
                .append(" ADD ").append(column.getName()).append(" ").append(toSqlType(column.getType()));

        try (PhoenixConnection connection = getConnection()) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public void dropColumn(PhoenixTableHandle handle, PhoenixColumnHandle columnHandle)
    {
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()))
                .append(" DROP COLUMN ").append(columnHandle.getColumnName());

        try (PhoenixConnection connection = getConnection()) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    public void rollbackCreateTable(PhoenixOutputTableHandle handle)
    {
        dropTable(new PhoenixTableHandle(
                handle.getConnectorId(),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTableName()));
    }

    protected ResultSet getTables(PhoenixConnection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {
                        "TABLE", "VIEW"
                });
    }

    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    protected void execute(PhoenixConnection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }

    public Map<String, Object> getTableProperties(PhoenixTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (PhoenixConnection pconn = getConnection(); HBaseAdmin admin = pconn.getQueryServices().getAdmin()) {
            PTable table = getTable(pconn, getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

            List<PColumn> pkColumns = table.getPKColumns();
            if (!pkColumns.isEmpty()) {
                properties.put(PhoenixTableProperties.ROWKEYS, table.getPKColumns().stream()
                        .map(PColumn::getName)
                        .map(PName::getString)
                        .filter(name -> !name.startsWith("_") && !ROWKEY.equals(name))
                        .map(name -> {
                            try {
                                if (table.getColumnForColumnName(name).isRowTimestamp()) {
                                    return name + " ROW_TIMESTAMP";
                                }
                            }
                            catch (ColumnNotFoundException | AmbiguousColumnException e) {
                            }
                            return name;
                        })
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

            properties.put(PhoenixTableProperties.UPDATE_CACHE_FREQUENCY, table.getUpdateCacheFrequency());

            HTableDescriptor tableDesc = admin.getTableDescriptor(table.getPhysicalName().getBytes());

            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (!columnFamily.getBloomFilterType().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.BLOOMFILTER, columnFamily.getBloomFilterType().toString());
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        properties.put(PhoenixTableProperties.VERSIONS, columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        properties.put(PhoenixTableProperties.MIN_VERSIONS, columnFamily.getMinVersions());
                    }
                    if (!columnFamily.getCompression().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.COMPRESSION, columnFamily.getCompression().toString());
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
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        return properties.build();
    }

    public static String getFullTableName(String catalog, String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(catalog).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(schema).append(".");
        }
        sb.append(table);
        return sb.toString();
    }

    protected static String toSqlType(Type type)
    {
        if (type instanceof VarcharType) {
            if (((VarcharType) type).isUnbounded()) {
                return "VARCHAR";
            }
            return "VARCHAR(" + ((VarcharType) type).getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "CHAR";
            }
            return "CHAR(" + ((CharType) type).getLength() + ")";
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }

        if (type instanceof DecimalType) {
            return type.toString().toUpperCase();
        }

        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);
            sqlType = toSqlType(elementType);
            return sqlType + " ARRAY[]";
        }

        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getTypeSignature());
    }

    protected static Type toPrestoType(int phoenixType, int columnSize, int decimalDigits, int arraySize, int rawTypeId)
    {
        switch (phoenixType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
                return DOUBLE;
            case Types.DECIMAL:
                return DecimalType.createDecimalType(columnSize, decimalDigits);
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize == 0 || columnSize > VarcharType.MAX_LENGTH) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
            case Types.ARRAY:
                PDataType<?> baseType = PDataType.fromTypeId(rawTypeId - PDataType.ARRAY_TYPE_BASE);
                Type basePrestoType = toPrestoType(baseType.getSqlType(), columnSize, decimalDigits, arraySize, rawTypeId);
                return new ArrayType(basePrestoType);
        }
        return null;
    }

    private static String escapeNamePattern(String name, String escape)
    {
        if ((name == null) || (escape == null)) {
            return name;
        }
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    private static ResultSet getColumns(PhoenixTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }
}
