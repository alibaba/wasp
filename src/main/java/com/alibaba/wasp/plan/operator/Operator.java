/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.wasp.plan.operator;

import com.alibaba.wasp.util.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic Operator.
 *
 */
public abstract class Operator implements Node {

  private List<Operator> childOperators = new ArrayList<Operator>();

  public void addChild(Operator op) {
    childOperators.add(op);
  }

  public void setChildOperators(List<Operator> childOperators) {
    this.childOperators = childOperators;
  }

  public List<Operator> getChildOperators() {
    return childOperators;
  }

  abstract public Object forward(Object row);

  @Override
  public List<? extends Node> getChildren() {
    return childOperators;
  }

}
