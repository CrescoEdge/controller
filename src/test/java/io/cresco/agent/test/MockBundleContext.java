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

import org.osgi.framework.*;

import java.io.File;
import java.io.InputStream;
import java.util.*;


/**
 * The <code>MockBundleContext</code> is a dummy implementation of the
 * <code>BundleContext</code> interface. No methods are implemented here, that
 * is all methods have no effect and return <code>null</code> if a return value
 * is specified.
 * <p>
 * Extensions may overwrite methods as see fit.
 */
public class MockBundleContext implements BundleContext
{

    private final Properties properties = new Properties();


    public void setProperty( String name, String value )
    {
        if ( value == null )
        {
            properties.remove( name );
        }
        else
        {
            properties.setProperty( name, value );
        }
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#addBundleListener(org.osgi.framework
     * .BundleListener)
     */
    @Override
    public void addBundleListener( BundleListener arg0 )
    {
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#addFrameworkListener(org.osgi.framework
     * .FrameworkListener)
     */
    @Override
    public void addFrameworkListener( FrameworkListener arg0 )
    {
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#addServiceListener(org.osgi.framework
     * .ServiceListener)
     */
    @Override
    public void addServiceListener( ServiceListener arg0 )
    {
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#addServiceListener(org.osgi.framework
     * .ServiceListener, java.lang.String)
     */
    @Override
    public void addServiceListener( ServiceListener arg0, String arg1 )
    {
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#createFilter(java.lang.String)
     */
    @Override
    public Filter createFilter( String arg0 )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#getAllServiceReferences(java.lang.String
     * , java.lang.String)
     */
    @Override
    public ServiceReference<?>[] getAllServiceReferences( String arg0, String arg1 )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#getBundle()
     */
    @Override
    public Bundle getBundle()
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#getBundle(long)
     */
    @Override
    public Bundle getBundle( long arg0 )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#getBundles()
     */
    @Override
    public Bundle[] getBundles()
    {

        List<Bundle> bundleList = new ArrayList<>();
        bundleList.add(new MockBundle("io.cresco.controller","1.0-SNAPSHOT"));
        bundleList.add(new MockBundle("io.cresco.repo","1.0-SNAPSHOT"));
        //dashboard is deprecated v1.1
        //bundleList.add(new MockBundle("io.cresco.dashboard","1.0-SNAPSHOT"));
        bundleList.add(new MockBundle("io.cresco.cep","1.0-SNAPSHOT"));
        bundleList.add(new MockBundle("io.cresco.sysinfo","1.0-SNAPSHOT"));

        return (Bundle[])bundleList.toArray();

    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#getDataFile(java.lang.String)
     */
    @Override
    public File getDataFile( String arg0 )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#getProperty(java.lang.String)
     */
    @Override
    public String getProperty( String name )
    {
        return properties.getProperty( name );
    }


    /*
     * (non-Javadoc)
     * @seeorg.osgi.framework.BundleContext#getService(org.osgi.framework.
     * ServiceReference)
     */
    @Override
    public <S> S getService( ServiceReference<S> reference )
    {

        String requestedRef = (String)reference.getProperty("name");

        System.out.println("reference requested : " + requestedRef);


        if(requestedRef.equals("org.osgi.service.cm.ConfigurationAdmin")) {
            MockConfigurationAdminImpl configurationAdmin = new MockConfigurationAdminImpl();
            return (S)configurationAdmin;
        } else {
            return null;
        }


    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#getServiceReference(java.lang.String)
     */
    @Override
    public ServiceReference<?> getServiceReference( String arg0 )
    {
        System.out.println("Service Ref Request: " + arg0);
        return new MockServiceReference("org.osgi.service.cm.ConfigurationAdmin");

    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#getServiceReferences(java.lang.String,
     * java.lang.String)
     */
    @Override
    public ServiceReference<?>[] getServiceReferences( String arg0, String arg1 )
    {
        System.out.println(arg0 + ":" + arg1);
        ServiceReference<?>[] serviceReferences = new ServiceReference<?>[1];
        serviceReferences[0] = new MockServiceReference(arg0);
        return serviceReferences;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#installBundle(java.lang.String)
     */
    @Override
    public Bundle installBundle( String arg0 )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#installBundle(java.lang.String,
     * java.io.InputStream)
     */
    @Override
    public Bundle installBundle( String arg0, InputStream arg1 )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#registerService(java.lang.String[],
     * java.lang.Object, java.util.Dictionary)
     */
    @Override
    public ServiceRegistration<?> registerService( String[] clazzes, Object service, Dictionary<String, ?> properties )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleContext#registerService(java.lang.String,
     * java.lang.Object, java.util.Dictionary)
     */
    @Override
    public ServiceRegistration<?> registerService( String clazz, Object service, Dictionary<String, ?> properties )
    {
        return null;
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#removeBundleListener(org.osgi.framework
     * .BundleListener)
     */
    @Override
    public void removeBundleListener( BundleListener arg0 )
    {
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#removeFrameworkListener(org.osgi.framework
     * .FrameworkListener)
     */
    @Override
    public void removeFrameworkListener( FrameworkListener arg0 )
    {
    }


    /*
     * (non-Javadoc)
     * @see
     * org.osgi.framework.BundleContext#removeServiceListener(org.osgi.framework
     * .ServiceListener)
     */
    @Override
    public void removeServiceListener( ServiceListener arg0 )
    {
    }


    /*
     * (non-Javadoc)
     * @seeorg.osgi.framework.BundleContext#ungetService(org.osgi.framework.
     * ServiceReference)
     */
    @Override
    public boolean ungetService( ServiceReference<?> reference )
    {
        return false;
    }


    @Override
    public <S> ServiceRegistration<S> registerService( Class<S> clazz, S service, Dictionary<String, ?> properties )
    {
        return null;
    }


    @Override
    public <S> ServiceReference<S> getServiceReference( Class<S> clazz )
    {
        return null;
    }


    @Override
    public <S> Collection<ServiceReference<S>> getServiceReferences( Class<S> clazz, String filter )
    {
        return null;
    }


    @Override
    public Bundle getBundle( String location )
    {
        return null;
    }


    @Override
    public <S> ServiceRegistration<S> registerService(Class<S> clazz, ServiceFactory<S> factory,
                                                      Dictionary<String, ?> properties)
    {
        return null;
    }


    @Override
    public <S> ServiceObjects<S> getServiceObjects(ServiceReference<S> reference)
    {
        return null;
    }
}