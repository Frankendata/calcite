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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

/**
 * Calling convention for relational operations that occur in a SubStrait
 * database.
 *
 * <p>The convention is a slight misnomer. The operations occur in whatever
 * data-flow architecture the database uses internally. Nevertheless, the result
 * pops out in SubStrait.</p>
 *
 * <p>This is the only convention, thus far, that is not a singleton. Each
 * instance contains a SubStrait schema (and therefore a data source). If Calcite is
 * working with two different databases, it would even make sense to convert
 * from "SubStrait#A" convention to "SubStrait#B", even though we don't do it currently.
 * (That would involve asking database B to open a database link to database
 * A.)</p>
 *
 * <p>As a result, converter rules from and to this convention need to be
 * instantiated, at the start of planning, for each SubStrait database in play.</p>
 */
public class SubStraitConvention extends Convention.Impl {
  /** Cost of a SubStrait node versus implementing an equivalent node in a "typical"
   * calling convention. */
  public static final double COST_MULTIPLIER = 0.8d;


  public SubStraitConvention(String name) {
    super("SUBSTRAIT." + name, SubStraitRel.class);
  }

  public static SubStraitConvention of(String name) {
    return new SubStraitConvention(name);
  }

  @Override public void register(RelOptPlanner planner) {
    for (RelOptRule rule : SubStraitRules.rules(this)) {
      planner.addRule(rule);
    }
    planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
    planner.addRule(CoreRules.PROJECT_REMOVE);
  }
}
