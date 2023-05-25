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

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import java.util.Map;

public class SubStraitSchemaFactory implements SchemaFactory {
  public static final SubStraitSchemaFactory INSTANCE = new SubStraitSchemaFactory();

  private SubStraitSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    TpchSchema tpchSchema = new TpchSchema(1D, 1, 1);
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    tpchSchema.getTableMap().forEach((k,v) ->{
      builder.put(k, new SubStraitTable());
    });
    Map<String, Table> tableMap = builder.build();

    SubStraitSchema tpchSubStraitSchema = new SubStraitSchema(tableMap);
    return tpchSubStraitSchema;
  }
}
