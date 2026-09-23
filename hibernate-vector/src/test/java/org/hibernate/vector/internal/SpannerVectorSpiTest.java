/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.vector.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.FunctionContributor;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SpannerDialect;
import org.hibernate.dialect.SpannerPostgreSQLDialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.service.ServiceRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test verifying that Cloud Spanner vector contributors (both GoogleSQL and PostgreSQL dialects)
 * are properly declared and discoverable via the Java {@link ServiceLoader} SPI mechanism.
 *
 * Covers:
 * 1. {@link TypeContributor} SPI discovery for {@link SpannerVectorTypeContributor} and {@link SpannerPostgreSQLVectorTypeContributor}.
 * 2. {@link FunctionContributor} SPI discovery for {@link SpannerVectorFunctionContributor} and {@link SpannerPostgreSQLVectorFunctionContributor}.
 * 3. Reflection-based instantiation, public constructor contracts, and contributor ordinals.
 * 4. Direct {@code META-INF/services} descriptor validation (formatting, class presence, type hierarchy).
 * 5. Dialect guard no-op validation for unrelated dialects.
 * 6. Cross-dialect mutual isolation verification between GoogleSQL and PostgreSQL contributors.
 * 7. Multi-ClassLoader ServiceLoader discovery verification (TCCL, custom URLClassLoader, TCCL dynamic switching).
 * 8. ServiceLoader concurrency and provider iteration stress testing.
 * 9. Exhaustive verification of all 10 registered TypeContributor and FunctionContributor providers.
 */
public class SpannerVectorSpiTest {

	private static final String TYPE_CONTRIBUTOR_SERVICE_FILE =
			"META-INF/services/org.hibernate.boot.model.TypeContributor";
	private static final String FUNCTION_CONTRIBUTOR_SERVICE_FILE =
			"META-INF/services/org.hibernate.boot.model.FunctionContributor";

	// ------------------------------------------------------------------------
	// 1. TypeContributor Discovery via ServiceLoader
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("ServiceLoader discovers and instantiates Spanner TypeContributors")
	public void testTypeContributorDiscoveryViaServiceLoader() {
		final ClassLoader classLoader = getClass().getClassLoader();
		final ServiceLoader<TypeContributor> loader = ServiceLoader.load( TypeContributor.class, classLoader );

		final List<Class<? extends TypeContributor>> discoveredTypes = loader.stream()
				.map( ServiceLoader.Provider::type )
				.collect( Collectors.toList() );

		assertTrue(
				discoveredTypes.contains( SpannerVectorTypeContributor.class ),
				"ServiceLoader must discover SpannerVectorTypeContributor"
		);
		assertTrue(
				discoveredTypes.contains( SpannerPostgreSQLVectorTypeContributor.class ),
				"ServiceLoader must discover SpannerPostgreSQLVectorTypeContributor"
		);

		// Iterate through all discovered services to ensure clean instantiation without ServiceConfigurationError
		final List<TypeContributor> instantiatedContributors = new ArrayList<>();
		for ( TypeContributor contributor : loader ) {
			assertNotNull( contributor, "Discovered TypeContributor instance must not be null" );
			instantiatedContributors.add( contributor );
		}

		final Optional<TypeContributor> spannerType = instantiatedContributors.stream()
				.filter( c -> c instanceof SpannerVectorTypeContributor )
				.findFirst();
		assertTrue( spannerType.isPresent(), "SpannerVectorTypeContributor instance must be present" );
		assertInstanceOf( SpannerVectorTypeContributor.class, spannerType.get() );

		final Optional<TypeContributor> spannerPgType = instantiatedContributors.stream()
				.filter( c -> c instanceof SpannerPostgreSQLVectorTypeContributor )
				.findFirst();
		assertTrue( spannerPgType.isPresent(), "SpannerPostgreSQLVectorTypeContributor instance must be present" );
		assertInstanceOf( SpannerPostgreSQLVectorTypeContributor.class, spannerPgType.get() );
	}

	// ------------------------------------------------------------------------
	// 2. FunctionContributor Discovery via ServiceLoader
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("ServiceLoader discovers and instantiates Spanner FunctionContributors")
	public void testFunctionContributorDiscoveryViaServiceLoader() {
		final ClassLoader classLoader = getClass().getClassLoader();
		final ServiceLoader<FunctionContributor> loader = ServiceLoader.load( FunctionContributor.class, classLoader );

		final List<Class<? extends FunctionContributor>> discoveredTypes = loader.stream()
				.map( ServiceLoader.Provider::type )
				.collect( Collectors.toList() );

		assertTrue(
				discoveredTypes.contains( SpannerVectorFunctionContributor.class ),
				"ServiceLoader must discover SpannerVectorFunctionContributor"
		);
		assertTrue(
				discoveredTypes.contains( SpannerPostgreSQLVectorFunctionContributor.class ),
				"ServiceLoader must discover SpannerPostgreSQLVectorFunctionContributor"
		);

		// Iterate through all discovered services to ensure clean instantiation without ServiceConfigurationError
		final List<FunctionContributor> instantiatedContributors = new ArrayList<>();
		for ( FunctionContributor contributor : loader ) {
			assertNotNull( contributor, "Discovered FunctionContributor instance must not be null" );
			instantiatedContributors.add( contributor );
		}

		final Optional<FunctionContributor> spannerFunc = instantiatedContributors.stream()
				.filter( c -> c instanceof SpannerVectorFunctionContributor )
				.findFirst();
		assertTrue( spannerFunc.isPresent(), "SpannerVectorFunctionContributor instance must be present" );
		assertInstanceOf( SpannerVectorFunctionContributor.class, spannerFunc.get() );

		final Optional<FunctionContributor> spannerPgFunc = instantiatedContributors.stream()
				.filter( c -> c instanceof SpannerPostgreSQLVectorFunctionContributor )
				.findFirst();
		assertTrue( spannerPgFunc.isPresent(), "SpannerPostgreSQLVectorFunctionContributor instance must be present" );
		assertInstanceOf( SpannerPostgreSQLVectorFunctionContributor.class, spannerPgFunc.get() );
	}

	// ------------------------------------------------------------------------
	// 3. Direct Instantiation, Public No-Arg Constructor & Ordinals
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("Spanner TypeContributors can be instantiated and have valid ordinals")
	public void testTypeContributorProperties() throws Exception {
		// Verify SpannerVectorTypeContributor
		Class<SpannerVectorTypeContributor> gsqlClass = SpannerVectorTypeContributor.class;
		assertTrue( Modifier.isPublic( gsqlClass.getModifiers() ), "Class must be public for ServiceLoader" );
		assertNotNull( gsqlClass.getConstructor(), "Class must have a public no-arg constructor" );

		SpannerVectorTypeContributor gsqlTypeContributor = gsqlClass.getDeclaredConstructor().newInstance();
		assertNotNull( gsqlTypeContributor );
		assertEquals( 1000, gsqlTypeContributor.ordinal(), "Default TypeContributor ordinal should be 1000" );

		// Verify SpannerPostgreSQLVectorTypeContributor
		Class<SpannerPostgreSQLVectorTypeContributor> pgClass = SpannerPostgreSQLVectorTypeContributor.class;
		assertTrue( Modifier.isPublic( pgClass.getModifiers() ), "Class must be public for ServiceLoader" );
		assertNotNull( pgClass.getConstructor(), "Class must have a public no-arg constructor" );

		SpannerPostgreSQLVectorTypeContributor pgTypeContributor = pgClass.getDeclaredConstructor().newInstance();
		assertNotNull( pgTypeContributor );
		assertEquals( 1000, pgTypeContributor.ordinal(), "Default TypeContributor ordinal should be 1000" );
	}

	@Test
	@DisplayName("Spanner FunctionContributors can be instantiated and have valid ordinals")
	public void testFunctionContributorProperties() throws Exception {
		// Verify SpannerPostgreSQLVectorFunctionContributor
		Class<SpannerPostgreSQLVectorFunctionContributor> pgClass = SpannerPostgreSQLVectorFunctionContributor.class;
		assertTrue( Modifier.isPublic( pgClass.getModifiers() ), "Class must be public for ServiceLoader" );
		assertNotNull( pgClass.getConstructor(), "Class must have a public no-arg constructor" );

		SpannerPostgreSQLVectorFunctionContributor pgFuncContributor = pgClass.getDeclaredConstructor().newInstance();
		assertNotNull( pgFuncContributor );
		assertEquals( 200, pgFuncContributor.ordinal(), "SpannerPostgreSQLVectorFunctionContributor ordinal must be 200" );

		// Verify SpannerVectorFunctionContributor
		Class<SpannerVectorFunctionContributor> gsqlClass = SpannerVectorFunctionContributor.class;
		assertTrue( Modifier.isPublic( gsqlClass.getModifiers() ), "Class must be public for ServiceLoader" );
		assertNotNull( gsqlClass.getConstructor(), "Class must have a public no-arg constructor" );

		SpannerVectorFunctionContributor gsqlFuncContributor = gsqlClass.getDeclaredConstructor().newInstance();
		assertNotNull( gsqlFuncContributor );
		assertEquals( 200, gsqlFuncContributor.ordinal(), "SpannerVectorFunctionContributor ordinal must be 200" );
	}

	// ------------------------------------------------------------------------
	// 4. META-INF/services Descriptor File Integrity
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("META-INF/services/org.hibernate.boot.model.TypeContributor contains valid class names")
	public void testTypeContributorServiceFileIntegrity() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final List<String> entries = readServiceEntries( TYPE_CONTRIBUTOR_SERVICE_FILE, classLoader );

		assertFalse( entries.isEmpty(), "TypeContributor service file must not be empty" );

		assertTrue(
				entries.contains( SpannerVectorTypeContributor.class.getName() ),
				"Service file must contain " + SpannerVectorTypeContributor.class.getName()
		);
		assertTrue(
				entries.contains( SpannerPostgreSQLVectorTypeContributor.class.getName() ),
				"Service file must contain " + SpannerPostgreSQLVectorTypeContributor.class.getName()
		);

		// Validate all entries in the file
		for ( String entry : entries ) {
			Class<?> clazz = Class.forName( entry, false, classLoader );
			assertTrue(
					TypeContributor.class.isAssignableFrom( clazz ),
					"Class " + entry + " must implement TypeContributor"
			);
		}
	}

	@Test
	@DisplayName("META-INF/services/org.hibernate.boot.model.FunctionContributor contains valid class names")
	public void testFunctionContributorServiceFileIntegrity() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final List<String> entries = readServiceEntries( FUNCTION_CONTRIBUTOR_SERVICE_FILE, classLoader );

		assertFalse( entries.isEmpty(), "FunctionContributor service file must not be empty" );

		assertTrue(
				entries.contains( SpannerVectorFunctionContributor.class.getName() ),
				"Service file must contain " + SpannerVectorFunctionContributor.class.getName()
		);
		assertTrue(
				entries.contains( SpannerPostgreSQLVectorFunctionContributor.class.getName() ),
				"Service file must contain " + SpannerPostgreSQLVectorFunctionContributor.class.getName()
		);

		// Validate all entries in the file
		for ( String entry : entries ) {
			Class<?> clazz = Class.forName( entry, false, classLoader );
			assertTrue(
					FunctionContributor.class.isAssignableFrom( clazz ),
					"Class " + entry + " must implement FunctionContributor"
			);
		}
	}

	// ------------------------------------------------------------------------
	// 5. Dialect Guard No-Op Verification for Unrelated Dialects
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("TypeContributors safely do nothing when dialect is not Spanner")
	public void testTypeContributorsNoOpForUnrelatedDialect() {
		final Dialect nonSpannerDialect = mock( Dialect.class );
		final JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( nonSpannerDialect );

		final ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		final TypeContributions typeContributions = mock( TypeContributions.class );

		// GoogleSQL contributor on non-Spanner dialect -> no-op
		new SpannerVectorTypeContributor().contribute( typeContributions, serviceRegistry );
		verify( typeContributions, never() ).getTypeConfiguration();

		// PostgreSQL contributor on non-Spanner dialect -> no-op
		new SpannerPostgreSQLVectorTypeContributor().contribute( typeContributions, serviceRegistry );
		verify( typeContributions, never() ).getTypeConfiguration();
	}

	@Test
	@DisplayName("SpannerPostgreSQLVectorFunctionContributor safely does nothing when dialect is not Spanner PostgreSQL")
	public void testPgFunctionContributorNoOpForUnrelatedDialect() {
		final Dialect nonSpannerPgDialect = mock( Dialect.class );
		final FunctionContributions functionContributions = mock( FunctionContributions.class );
		when( functionContributions.getDialect() ).thenReturn( nonSpannerPgDialect );

		new SpannerPostgreSQLVectorFunctionContributor().contributeFunctions( functionContributions );
		verify( functionContributions, never() ).getFunctionRegistry();
	}

	@Test
	@DisplayName("SpannerVectorFunctionContributor safely does nothing when dialect is not Spanner GoogleSQL")
	public void testGoogleSqlFunctionContributorNoOpForUnrelatedDialect() {
		final Dialect nonSpannerDialect = mock( Dialect.class );
		final FunctionContributions functionContributions = mock( FunctionContributions.class );
		when( functionContributions.getDialect() ).thenReturn( nonSpannerDialect );

		new SpannerVectorFunctionContributor().contributeFunctions( functionContributions );
		verify( functionContributions, never() ).getFunctionRegistry();
	}

	// ------------------------------------------------------------------------
	// 6. Cross-Dialect Mutual Isolation Verification
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("GoogleSQL contributors do not register anything when dialect is Spanner PostgreSQL")
	public void testGoogleSqlContributorsDoNotRegisterForSpannerPostgreSQLDialect() {
		final SpannerPostgreSQLDialect pgDialect = mock( SpannerPostgreSQLDialect.class );

		// Verify TypeContributor isolation
		final JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( pgDialect );

		final ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		final TypeContributions typeContributions = mock( TypeContributions.class );
		new SpannerVectorTypeContributor().contribute( typeContributions, serviceRegistry );
		verify( typeContributions, never() ).getTypeConfiguration();

		// Verify FunctionContributor isolation
		final FunctionContributions functionContributions = mock( FunctionContributions.class );
		when( functionContributions.getDialect() ).thenReturn( pgDialect );

		new SpannerVectorFunctionContributor().contributeFunctions( functionContributions );
		verify( functionContributions, never() ).getFunctionRegistry();
	}

	@Test
	@DisplayName("PostgreSQL contributors do not register anything when dialect is Spanner GoogleSQL")
	public void testPgContributorsDoNotRegisterForSpannerGoogleSqlDialect() {
		final SpannerDialect gsqlDialect = mock( SpannerDialect.class );

		// Verify TypeContributor isolation
		final JdbcServices jdbcServices = mock( JdbcServices.class );
		when( jdbcServices.getDialect() ).thenReturn( gsqlDialect );

		final ServiceRegistry serviceRegistry = mock( ServiceRegistry.class );
		when( serviceRegistry.requireService( JdbcServices.class ) ).thenReturn( jdbcServices );

		final TypeContributions typeContributions = mock( TypeContributions.class );
		new SpannerPostgreSQLVectorTypeContributor().contribute( typeContributions, serviceRegistry );
		verify( typeContributions, never() ).getTypeConfiguration();

		// Verify FunctionContributor isolation
		final FunctionContributions functionContributions = mock( FunctionContributions.class );
		when( functionContributions.getDialect() ).thenReturn( gsqlDialect );

		new SpannerPostgreSQLVectorFunctionContributor().contributeFunctions( functionContributions );
		verify( functionContributions, never() ).getFunctionRegistry();
	}

	// ------------------------------------------------------------------------
	// 7. Multi-ClassLoader ServiceLoader Discovery Verification
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("ServiceLoader discovers Spanner contributors using Thread Context ClassLoader")
	public void testDiscoveryViaThreadContextClassLoader() {
		final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
		assertNotNull( tccl, "Thread context class loader must not be null" );

		final ServiceLoader<TypeContributor> typeLoader = ServiceLoader.load( TypeContributor.class, tccl );
		final List<Class<? extends TypeContributor>> discoveredTypes = typeLoader.stream()
				.map( ServiceLoader.Provider::type )
				.collect( Collectors.toList() );
		assertTrue(
				discoveredTypes.contains( SpannerVectorTypeContributor.class ),
				"TCCL ServiceLoader must discover SpannerVectorTypeContributor"
		);
		assertTrue(
				discoveredTypes.contains( SpannerPostgreSQLVectorTypeContributor.class ),
				"TCCL ServiceLoader must discover SpannerPostgreSQLVectorTypeContributor"
		);

		final ServiceLoader<FunctionContributor> funcLoader = ServiceLoader.load( FunctionContributor.class, tccl );
		final List<Class<? extends FunctionContributor>> discoveredFuncs = funcLoader.stream()
				.map( ServiceLoader.Provider::type )
				.collect( Collectors.toList() );
		assertTrue(
				discoveredFuncs.contains( SpannerVectorFunctionContributor.class ),
				"TCCL ServiceLoader must discover SpannerVectorFunctionContributor"
		);
		assertTrue(
				discoveredFuncs.contains( SpannerPostgreSQLVectorFunctionContributor.class ),
				"TCCL ServiceLoader must discover SpannerPostgreSQLVectorFunctionContributor"
		);
	}

	@Test
	@DisplayName("ServiceLoader discovers Spanner contributors under custom URLClassLoader and TCCL switching")
	public void testDiscoveryViaCustomUrlClassLoaderAndTcclSwitching() throws Exception {
		final ClassLoader currentCl = getClass().getClassLoader();
		final URL typeContributorUrl = currentCl.getResource( TYPE_CONTRIBUTOR_SERVICE_FILE );
		final URL funcContributorUrl = currentCl.getResource( FUNCTION_CONTRIBUTOR_SERVICE_FILE );
		assertNotNull( typeContributorUrl, "TypeContributor service file must exist on classpath" );
		assertNotNull( funcContributorUrl, "FunctionContributor service file must exist on classpath" );

		final URL[] urls = new URL[] {
				SpannerVectorTypeContributor.class.getProtectionDomain().getCodeSource().getLocation(),
				TypeContributor.class.getProtectionDomain().getCodeSource().getLocation()
		};

		try ( URLClassLoader customLoader = new URLClassLoader( urls, currentCl ) ) {
			final ServiceLoader<TypeContributor> customTypeLoader =
					ServiceLoader.load( TypeContributor.class, customLoader );
			final List<Class<? extends TypeContributor>> customDiscoveredTypes = customTypeLoader.stream()
					.map( ServiceLoader.Provider::type )
					.collect( Collectors.toList() );
			assertTrue( customDiscoveredTypes.contains( SpannerVectorTypeContributor.class ) );
			assertTrue( customDiscoveredTypes.contains( SpannerPostgreSQLVectorTypeContributor.class ) );

			// Test TCCL switching behavior
			final ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader( customLoader );
				final ServiceLoader<TypeContributor> tcclLoader = ServiceLoader.load( TypeContributor.class );
				final List<Class<? extends TypeContributor>> tcclTypes = tcclLoader.stream()
						.map( ServiceLoader.Provider::type )
						.collect( Collectors.toList() );
				assertTrue( tcclTypes.contains( SpannerVectorTypeContributor.class ) );
				assertTrue( tcclTypes.contains( SpannerPostgreSQLVectorTypeContributor.class ) );
			}
			finally {
				Thread.currentThread().setContextClassLoader( originalTccl );
			}
		}
	}

	// ------------------------------------------------------------------------
	// 8. ServiceLoader Concurrency and Thread Safety Stress Testing
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("ServiceLoader concurrency and provider iteration stress test")
	public void testServiceLoaderConcurrencyAndProviderIteration() throws Exception {
		final int threads = 16;
		final int iterationsPerThread = 50;
		final ClassLoader classLoader = getClass().getClassLoader();
		final ExecutorService executor = Executors.newFixedThreadPool( threads );
		final CountDownLatch startLatch = new CountDownLatch( 1 );
		final List<Future<?>> futures = new ArrayList<>();

		for ( int i = 0; i < threads; i++ ) {
			futures.add( executor.submit( () -> {
				startLatch.await();
				for ( int iter = 0; iter < iterationsPerThread; iter++ ) {
					// 1. TypeContributor concurrent discovery and instantiation
					final ServiceLoader<TypeContributor> typeLoader =
							ServiceLoader.load( TypeContributor.class, classLoader );
					final List<TypeContributor> typeInstances = new ArrayList<>();
					for ( TypeContributor tc : typeLoader ) {
						assertNotNull( tc );
						typeInstances.add( tc );
					}
					assertTrue( typeInstances.size() >= 10, "Must instantiate at least 10 TypeContributors" );
					assertTrue( typeInstances.stream().anyMatch( c -> c instanceof SpannerVectorTypeContributor ) );
					assertTrue( typeInstances.stream().anyMatch( c -> c instanceof SpannerPostgreSQLVectorTypeContributor ) );

					// 2. FunctionContributor concurrent discovery and instantiation
					final ServiceLoader<FunctionContributor> funcLoader =
							ServiceLoader.load( FunctionContributor.class, classLoader );
					final List<FunctionContributor> funcInstances = new ArrayList<>();
					for ( FunctionContributor fc : funcLoader ) {
						assertNotNull( fc );
						funcInstances.add( fc );
					}
					assertTrue( funcInstances.size() >= 10, "Must instantiate at least 10 FunctionContributors" );
					assertTrue( funcInstances.stream().anyMatch( c -> c instanceof SpannerVectorFunctionContributor ) );
					assertTrue( funcInstances.stream().anyMatch( c -> c instanceof SpannerPostgreSQLVectorFunctionContributor ) );
				}
				return null;
			} ) );
		}

		startLatch.countDown();
		for ( Future<?> future : futures ) {
			future.get( 60, TimeUnit.SECONDS );
		}
		executor.shutdown();
		assertTrue( executor.awaitTermination( 5, TimeUnit.SECONDS ) );
	}

	// ------------------------------------------------------------------------
	// 9. Exhaustive Verification of All 10 Registered Providers
	// ------------------------------------------------------------------------

	@Test
	@DisplayName("All 10 registered TypeContributor providers exist, are public with default constructors, and instantiate cleanly")
	public void testAllTenTypeContributorsInstantiationAndContracts() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final List<String> entries = readServiceEntries( TYPE_CONTRIBUTOR_SERVICE_FILE, classLoader );

		assertEquals( 10, entries.size(), "TypeContributor service file must contain exactly 10 provider classes" );

		for ( String className : entries ) {
			final Class<?> clazz = Class.forName( className, true, classLoader );
			assertTrue( Modifier.isPublic( clazz.getModifiers() ), "Class " + className + " must be public" );
			assertFalse( Modifier.isAbstract( clazz.getModifiers() ), "Class " + className + " must not be abstract" );
			assertFalse( clazz.isInterface(), "Class " + className + " must not be an interface" );

			final Constructor<?> constructor = clazz.getConstructor();
			assertNotNull( constructor, "Class " + className + " must have a public no-argument constructor" );
			assertTrue( Modifier.isPublic( constructor.getModifiers() ), "Constructor for " + className + " must be public" );

			final Object instance = constructor.newInstance();
			assertNotNull( instance, "Instantiated object must not be null for " + className );
			assertInstanceOf( TypeContributor.class, instance, "Object must implement TypeContributor: " + className );

			final TypeContributor contributor = (TypeContributor) instance;
			assertTrue( contributor.ordinal() >= 0, "Ordinal must be non-negative for " + className );
		}
	}

	@Test
	@DisplayName("All 10 registered FunctionContributor providers exist, are public with default constructors, and instantiate cleanly")
	public void testAllTenFunctionContributorsInstantiationAndContracts() throws Exception {
		final ClassLoader classLoader = getClass().getClassLoader();
		final List<String> entries = readServiceEntries( FUNCTION_CONTRIBUTOR_SERVICE_FILE, classLoader );

		assertEquals( 10, entries.size(), "FunctionContributor service file must contain exactly 10 provider classes" );

		for ( String className : entries ) {
			final Class<?> clazz = Class.forName( className, true, classLoader );
			assertTrue( Modifier.isPublic( clazz.getModifiers() ), "Class " + className + " must be public" );
			assertFalse( Modifier.isAbstract( clazz.getModifiers() ), "Class " + className + " must not be abstract" );
			assertFalse( clazz.isInterface(), "Class " + className + " must not be an interface" );

			final Constructor<?> constructor = clazz.getConstructor();
			assertNotNull( constructor, "Class " + className + " must have a public no-argument constructor" );
			assertTrue( Modifier.isPublic( constructor.getModifiers() ), "Constructor for " + className + " must be public" );

			final Object instance = constructor.newInstance();
			assertNotNull( instance, "Instantiated object must not be null for " + className );
			assertInstanceOf( FunctionContributor.class, instance, "Object must implement FunctionContributor: " + className );

			final FunctionContributor contributor = (FunctionContributor) instance;
			assertEquals( 200, contributor.ordinal(), "FunctionContributor " + className + " ordinal must be exactly 200" );
		}
	}

	// ------------------------------------------------------------------------
	// Helper Methods
	// ------------------------------------------------------------------------

	private static List<String> readServiceEntries(String resourcePath, ClassLoader classLoader) throws IOException {
		final Enumeration<URL> resources = classLoader.getResources( resourcePath );
		final List<String> lines = new ArrayList<>();
		while ( resources.hasMoreElements() ) {
			URL url = resources.nextElement();
			try ( InputStream in = url.openStream();
				BufferedReader reader = new BufferedReader( new InputStreamReader( in, StandardCharsets.UTF_8 ) ) ) {
				String line;
				while ( ( line = reader.readLine() ) != null ) {
					line = line.trim();
					// Skip comments and blank lines as defined by Java SPI spec
					int commentIdx = line.indexOf( '#' );
					if ( commentIdx >= 0 ) {
						line = line.substring( 0, commentIdx ).trim();
					}
					if ( !line.isEmpty() ) {
						lines.add( line );
					}
				}
			}
		}
		return lines;
	}
}
