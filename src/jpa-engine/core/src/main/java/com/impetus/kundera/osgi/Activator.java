package com.impetus.kundera.osgi;

import com.impetus.kundera.KunderaPersistence;
import java.util.Hashtable;
import javax.persistence.spi.PersistenceProvider;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * Used to discover/resolve JPA providers in an OSGi environment.
 *
 * @author v.dzalbo
 */
public class Activator implements BundleActivator {

    // following is so Aries can find and extend us for OSGi RFC 143
    public static final String PERSISTENCE_PROVIDER_ARIES = "javax.persistence.provider";
    // following would be set by Aries to expose their OSGi enabled provider
    public static final String PERSISTENCE_PROVIDER = PersistenceProvider.class.getName();
    public static final String OSGI_PERSISTENCE_PROVIDER = KunderaPersistence.class.getName();
    private static BundleContext ctx = null;
    private static ServiceRegistration svcReg = null;

    @Override
    public void start(BundleContext ctx) throws Exception {
	
	
	this.ctx = ctx;
	PersistenceProvider provider = new KunderaPersistence();
	Hashtable<String, String> props = new Hashtable<String, String>();
	// Aries queries for service providers by property "javax.persistence.provider"
	props.put(PERSISTENCE_PROVIDER_ARIES, OSGI_PERSISTENCE_PROVIDER);
	// The persistence service tracker in the geronimo spec api bundle examines
	// the property named "javax.persistence.PersistenceProvider" rather than
	// the the property provided for Aries.  In order to properly track the Kundera 
	// provider, this property must be set upon service registration.
	props.put(PERSISTENCE_PROVIDER, OSGI_PERSISTENCE_PROVIDER);
	svcReg = ctx.registerService(PERSISTENCE_PROVIDER, provider, props);

    }

    @Override
    public void stop(BundleContext ctx) throws Exception {
	if (svcReg != null) {
	    svcReg.unregister();
	    svcReg = null;
	}
	this.ctx = null;

    }

}
