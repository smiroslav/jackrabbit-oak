/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;

import javax.jcr.PropertyType;

final class DoubleValue extends MemoryValue {

    private final double value;

    public DoubleValue(double value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return PropertyType.DOUBLE;
    }

    @Override
    public BigDecimal getDecimal() {
        return new BigDecimal(value);
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public long getLong() {
        return (long) value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

}
