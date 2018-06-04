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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableUtils
{
    private static final Splitter DYNAMIC_COLUMN_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final Splitter DYNAMIC_COLUMN_TYPE_SPLITTER = Splitter.on(' ').trimResults().omitEmptyStrings();

    private TableUtils()
    {
    }

    public static String normalizeTableName(String tableName)
    {
        int dynamicColumnsIndex = tableName.indexOf('$');
        if (dynamicColumnsIndex > -1) {
            // remove dynamic columns.
            return tableName.substring(0, dynamicColumnsIndex);
        }

        return tableName;
    }

    public static List<PhoenixColumnHandle> getDynamicColumnHandles(String connectorId, TypeManager typeManager, String tableName)
    {
        ImmutableList.Builder<PhoenixColumnHandle> columnHandles = ImmutableList.builder();

        int dynamicColumnsIndex = tableName.indexOf('$');
        if (dynamicColumnsIndex > -1) {
            String dynamicColumns = tableName.substring(dynamicColumnsIndex + 1);
            for (String dynamicColumn : DYNAMIC_COLUMN_SPLITTER.split(dynamicColumns)) {
                ImmutableList<String> columnSignature = ImmutableList.copyOf(DYNAMIC_COLUMN_TYPE_SPLITTER.split(dynamicColumn));
                checkArgument(columnSignature.size() == 2, "Invalid dynamic columns %s", dynamicColumn);

                String columnName = columnSignature.get(0);
                Type columnType = typeManager.getType(parseTypeSignature(columnSignature.get(1)));
                requireNonNull(columnType, () -> format("TypeManager returned a null record set provider: %s", columnSignature.get(1)));

                columnHandles.add(new PhoenixColumnHandle(connectorId, columnName, columnType));
            }
        }
        return columnHandles.build();
    }
}
