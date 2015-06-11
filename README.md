## LinkETL

LinkETL is a model-driven dynamically-configurable framework to acquire data from external sources and save it in your application database. It can reuse your existing ORM mapping for the _target_ database, reducing configuration to just describing the _source_. It supports JDBC, LDAP, XML, CSV sources out of the box.

It is an ideal tool for domain-driven application designs, allowing to connect multiple data models together in a flexible way that allows independent changes between sources and targets.


## Getting started:

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

