[![Build Status](https://travis-ci.org/nhl/link-move.svg?branch=master)](https://travis-ci.org/nhl/link-move)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.nhl.link.move/link-move/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.nhl.link.move/link-move/)

## LinkMove

LinkMove is a model-driven dynamically-configurable framework to acquire data from external sources and save it in your 
database. Its primary motivation is to facilitate 
[domain-driven design](https://en.wikipedia.org/wiki/Domain-driven_design) architectures. In DDD terms LinkMove is a 
tool to synchronize data between related models from different ["bounded contexts"](http://martinfowler.com/bliki/BoundedContext.html). 
It can also be used as a general purpose ETL framework.

LinkMove connects data models in a flexible way that anticipates independent changes between sources and targets. It 
will reuse your existing ORM mapping for the _target_ database, reducing configuration to just describing the _source_. 
It supports JDBC, XML, JSON, CSV sources out of the box.

## Support

There are two options:

* Open an issue on GitHub with a label of "help wanted" or "question" (or "bug" if you think you found a bug).
* Post your question on the [LinkMove forum](https://groups.google.com/forum/?#!forum/linkmove-user).

## Getting Started

Add LinkMove dependency:
```XML
<dependency>
    <groupId>com.nhl.link.move</groupId>
    <artifactId>link-move</artifactId>
    <version>3.0.M5</version>
</dependency>
```
The core module above supports relational and XML sources. The following optional modules may be added if you need to work with other formats:

```XML
<!-- for JSON -->
<dependency>
    <groupId>com.nhl.link.move</groupId>
    <artifactId>link-move-json</artifactId>
    <version>3.0.M5</version>
</dependency>
```
```XML
<!-- for CSV -->
<dependency>
    <groupId>com.nhl.link.move</groupId>
    <artifactId>link-move-csv</artifactId>
    <version>3.0.M5</version>
</dependency>
```
Use it:

```Java
// bootstrap shared runtime that will run tasks
DataSource srcDS = // define how you'd connect to data source 
ServerRuntime targetRuntime = // Cayenne setup for data target .. targets are mapped in Cayenne 
File rootDir = .. // this is a parent dir of XML descriptors

LmRuntime lm = LmRuntime.builder()
          .connector(JdbcConnector.class, "myconnector", new DataSourceConnector(srcDS))
          .targetRuntime(targetRuntime)
          .extractorModelsRoot(rootDir)
          .build();

// create a reusable task for a given transformation
LmTask task = lm.getTaskService()
         .createOrUpdate(MyTargetEntity.class)
         .sourceExtractor("my-etl.xml")
         .matchBy(MyTargetEntity.NAME).task();

// run task, e.g. in a scheduled job
Execution e = task.run();
```

## Extractor XML Format

Extractor XML format is described by a formal schema: http://linkmove.io/xsd/extractor_config_2.xsd

An example using JDBC connector for the source data:

```XML
<?xml version="1.0" encoding="utf-8"?>
<config xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="https://nhl.github.io/link-move/xsd/extractor_config_3.xsd https://nhl.github.io/link-move/xsd/extractor_config_3.xsd"
        xmlns="https://nhl.github.io/link-move/xsd/extractor_config_3.xsd">
	
	<type>jdbc</type>
	<connectorId>myconnector</connectorId>
	
	<extractor>
		<!-- Optional source to target attribute mapping -->
		<attributes>
			<attribute>
				<type>java.lang.Integer</type>
				<source>AGE</source>
				<target>db:age</target>
			</attribute>
			<attribute>
				<type>java.lang.String</type>
				<source>DESCRIPTION</source>
				<target>db:description</target>
			</attribute>
			<attribute>
				<type>java.lang.String</type>
				<source>NAME</source>
				<target>db:name</target>
			</attribute>
		</attributes>
		<!-- JDBC connector properties. -->
		<properties>
			<!-- Query to run against the source. Supports full Cayenne 
			     SQLTemplate syntax, including parameters and directives.
			-->
			<extractor.jdbc.sqltemplate>
			       SELECT age, description, name FROM etl1
			</extractor.jdbc.sqltemplate>
		</properties>
	</extractor>
</config>
```

## Logging Configuration

LinkMove uses Slf4J abstraction for logging, that will work with most common logging frameworks (Log4J2, Logback, etc.).
With any framework you use, you will need to configure the following log levels depending on the desired verbosity of
your ETL tasks.

### Logging ETL Progress

You need to configure the `com.nhl.link.move.log` logger to log the progress of the ETL tasks. The following table 
shows what is logged at each log level:

|Log Level| What is Logged                                                                |
|---------|-------------------------------------------------------------------------------|
| WARN    | Nothing                                                                       |
| INFO    | Task start/stop with stats                                                    |
| DEBUG   | Same as INFO, but also includes start/stop of each segment with segment stats |
|TRACE|Same as DEBUG, but also includes IDs of all affected target objects (deleted, created, updated)|



### Logging SQL

ETL-related SQL generated by Cayenne is extremely verbose and barely human-readable. You need to configure the 
`org.apache.cayenne.log` logger to turn it on and off:

|Log Level| What is Logged                                                          |
|---------|-------------------------------------------------------------------------|
| WARN    | Nothing                                                                 |
| INFO    | Cayenne-generated SQL queries and updates|
