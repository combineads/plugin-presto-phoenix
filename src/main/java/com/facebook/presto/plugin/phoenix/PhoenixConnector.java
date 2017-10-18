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

import com.facebook.presto.plugin.phoenix.PhoenixConnector;
import com.facebook.presto.plugin.phoenix.PhoenixMetadata;
import com.facebook.presto.plugin.phoenix.PhoenixMetadataFactory;
import com.facebook.presto.plugin.phoenix.PhoenixRecordSinkProvider;
import com.facebook.presto.plugin.phoenix.PhoenixTransactionHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PhoenixConnector
        implements Connector
{
    private static final Logger log = Logger.get(PhoenixConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final PhoenixMetadataFactory phoenixMetadataFactory;
    private final PhoenixSplitManager phoenixSplitManager;
    private final PhoenixRecordSetProvider phoenixRecordSetProvider;
    private final PhoenixRecordSinkProvider phoenixRecordSinkProvider;
    private final PhoenixTableProperties phoenixTableProperties;

    private final ConcurrentMap<ConnectorTransactionHandle, PhoenixMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public PhoenixConnector(
            LifeCycleManager lifeCycleManager,
            PhoenixMetadataFactory phoenixMetadataFactory,
            PhoenixSplitManager phoenixSplitManager,
            PhoenixRecordSetProvider phoenixRecordSetProvider,
            PhoenixRecordSinkProvider phoenixRecordSinkProvider,
            PhoenixTableProperties phoenixTableProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.phoenixMetadataFactory = requireNonNull(phoenixMetadataFactory, "phoenixMetadataFactory is null");
        this.phoenixSplitManager = requireNonNull(phoenixSplitManager, "phoenixSplitManager is null");
        this.phoenixRecordSetProvider = requireNonNull(phoenixRecordSetProvider, "phoenixRecordSetProvider is null");
        this.phoenixRecordSinkProvider = requireNonNull(phoenixRecordSinkProvider, "phoenixRecordSinkProvider is null");
        this.phoenixTableProperties = requireNonNull(phoenixTableProperties, "phoenixTableProperties is null");
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        PhoenixTransactionHandle transaction = new PhoenixTransactionHandle();
        transactions.put(transaction, phoenixMetadataFactory.create());
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        PhoenixMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        PhoenixMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return phoenixSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return phoenixRecordSetProvider;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return phoenixRecordSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return phoenixTableProperties.getTableProperties();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
