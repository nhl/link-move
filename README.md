## LinkETL

LinkETL is a model-driven dynamically-configurable framework to acquire data from external sources and save it in your application database. It can reuse your existing ORM mapping for the _target_ database, reducing configuration to just describing the _source_. It supports JDBC, LDAP, XML, CSV sources out of the box.

It is an ideal tool for domain-driven application designs, allowing to connect multiple data models together in a flexible way that allows independent changes between sources and targets.


## Getting Started:

```Java
// bootstrap shared runtime that will run tasks
DataSource srcDS = // define how you'd connect to  ETL source 
ServerRuntime targetRuntime = // Cayenne setup for ETL target .. targets are mapped in Cayenne 
File rootDir = .. // this is a parent dir of XML descriptors

EtlRuntime etl = EtlRuntimeBuilder()
          .withConnector("myconnector", new DataSourceConnector(srcDS))
          .withTargetRuntime(targetRuntime)
          .extractorModelsRoot(rootDir)
          .build();

// create a reusable task for a given transformation
EtlTask task = etl.getTaskService()
         .createOrUpdate(MyTargetEntity.class)
         .sourceExtractor("my-etl.xml")
         .matchBy(MyTargetEntity.NAME).task();

// run task, e.g. in a scheduled job
Execution e = task.run();
```

## Extractor XML Format

Extractor XML format is described by a formal schema: http://nhl.github.io/link-etl/xsd/extractor_config_2.xsd

A JDBC source example:

```XML
<?xml version="1.0" encoding="utf-8"?>
<config xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://nhl.github.io/link-etl/xsd/extractor_config_2.xsd"
	xmlns="http://nhl.github.io/link-etl/xsd/extractor_config_2.xsd">
	<type>jdbc</type>
	<connectorId>derbysrc</connectorId>
	<extractor>
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
		<properties>
			<extractor.jdbc.sqltemplate>
			       SELECT age, description, name FROM utest.etl1
			</extractor.jdbc.sqltemplate>
		</properties>
	</extractor>
</config>
```


