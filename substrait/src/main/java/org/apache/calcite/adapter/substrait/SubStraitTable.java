/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.substrait;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import static org.apache.calcite.adapter.substrait.SubStraitSchema.subStraitConvention;

public class SubStraitTable extends AbstractTable implements TranslatableTable {

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return null;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new SubStraitTableScan(context.getCluster(), context.getTableHints(), relOptTable, this, subStraitConvention);
  }
}
