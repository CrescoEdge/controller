package io.cresco.agent.test;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.osgi.framework.Bundle;
import org.osgi.framework.ServiceReference;

import java.util.HashMap;
import java.util.Map;


public class MockServiceReference<S> implements ServiceReference<S>
{

    private Map<String,String> properties;


    public MockServiceReference(Map<String,String> properties) {
        this.properties = properties;
    }

    public MockServiceReference(String name) {
        this.properties = new HashMap<>();
        properties.put("name",name);
    }

    @Override
    public Object getProperty( String key )
    {
        if(properties.containsKey(key)) {
            return properties.get(key);
        } else {
            return null;
        }
    }


    @Override
    public String[] getPropertyKeys()
    {
        return (String[])properties.keySet().toArray();
    }


    @Override
    public Bundle getBundle()
    {
        return null;
    }


    @Override
    public Bundle[] getUsingBundles()
    {
        return null;
    }


    @Override
    public boolean isAssignableTo( Bundle bundle, String className )
    {
        return true;
    }


    @Override
    public int compareTo( Object reference )
    {
        return 0;
    }

}