_This document contains upgrade notes for LinkMove 3.x and newer. Older versions are documented in
[UPGRADE-NOTES-1-2](./UPGRADE-NOTES-1-to-2.md)._

## Upgrading to 3.0.0-RC1

### Upgrade to DFLib 1.2 [#234](https://github.com/nhl/link-move/issues/234)
This upgrade to the latest stable DFLib may result in compile errors if custom LinkMove callbacks are using `DataFrame`
APIs directly. The first thing you may notice is package relocation (you will need to replace `import com.nhl.dflib.*` 
with `import org.dflib.*`), but `DataFrame` API itself was also significantly reworked. Please check the latest `1.x`
[DFLib documentation](https://dflib.org/dflib/docs/1.x/) to figure out how to perform your tasks with the new API 
or send a note to the [DFLib support forum](https://github.com/dflib/dflib/discussions).

## Upgrading to 3.0.M5

### Extractor Schema v3 [#221](https://github.com/nhl/link-move/issues/221) A new v3 of the extractor 
XML schema is published. It is identical for v2. The main reason for update is that we are no longer 
hosting linkmove.io website. So the namespace and location of the schema were updated to reflect the 
new web address at https://nhl.github.io/link-move/ . You can keep using v2 without any changes, but
we'd recommend an upgrade to v3 wne practical to reduce confusion:

```xml
<config 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xsi:schemaLocation="https://nhl.github.io/link-move/xsd/extractor_config_3.xsd https://nhl.github.io/link-move/xsd/extractor_config_3.xsd"
    xmlns="https://nhl.github.io/link-move/xsd/extractor_config_3.xsd">
</config>
```

## Upgrading to 3.0.M4

### Tracking Connectors by type [#222](https://github.com/nhl/link-move/issues/222)
* `LmRuntimeBuilder.withConnector(id, connector)` no longer makes sense and is replaced with 
`LmRuntimeBuilder.connector(connectorType, id, connector)`.
* `IConnectorFactory` interface is altered to provide extra metadata about the factory and to allow factory composition
* `IConnectorFactory` factories are now injected as a List, not a Map.
* `URIConnectorFactory`is deprecated, as the factory treats "connectorId" as a URI,  and this pattern is strongly 
discouraged. Instead, "connectorId" should be a symbolic name resolvable outside LinkMove. Also, `URIConnector` is
deprecated in favor of `URLConnector`.

## Upgrading to 3.0.M1

### Switch to Java 11 [#199](https://github.com/nhl/link-move/issues/199)
Java 11 is the minimal required Java version. If you are still on Java 8, please continue using LinkMove 2.x.

### Switch to Cayenne 4.2 [#200](https://github.com/nhl/link-move/issues/200)
The new version of Cayenne used by LinkMove is 4.2 (upgraded from the ancient 4.0.2). You will need to upgrade
your application Cayenne projects.

### Removed generic types from Tasks and Segments [#207](https://github.com/nhl/link-move/issues/207)
Now that LinkMove is using DataFrames as its internal data representation format, generic types of tasks and segments
make little sense and don't really help in their configuration and use. So the generic parameters were removed. This
will result in compilation errors in listeners that accept segments as method parameters. Listeners must update their 
method signatures, removing <T>.

### JdbcExtractor: column names are no longer converted to uppercase by default [#210](https://github.com/nhl/link-move/issues/210)
Since we started to support mixed result column names in SQL, there's no more implicit conversion of the JDBC result 
labels to uppercase. If you were not using an explicit `<extractor.jdbc.sqltemplate.caps/>` property in your JDBC 
extractor files, you may see errors like `JdbcRowReader Key is missing in the source 'SOME_COLUMN' ... ignoring`, and 
ultimately get null values in those columns. If your DB and extractors are affected, make sure to set this extractor 
property. E.g. `<extractor.jdbc.sqltemplate.caps>UPPER</extractor.jdbc.sqltemplate.caps>`
