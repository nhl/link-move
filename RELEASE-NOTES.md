## Release 3.0.M1

* #199 Require Java 11
* #200 Upgrade to Cayenne 4.2
* #201 Upgrade Jackson to 2.13.4
* #204 Upgrade jackson-databind to 2.13.4.2
* #205 Upgrade to DFLib 0.15

## Release 2.18

* #194 @AfterFksResolved callback is not invoked

## Release 2.16, 2.17

* #192 @AfterSourceRowsExtracted annotation

## Release 2.15

* #190 Bootique dependency leaks via BOM impor
* #191 Upgrade to DFLib 0.12

## Release 2.14

* #185 Target entities with flattened attributes
* #186 Upgrade to DFLib 0.10 
* #187 Binary compatibility with Cayenne 4.0, 4.1 and 4.2

## Release 2.13

* #183 "Will not perform lossy conversion from LocalTime" exception when lm'ing time

## Release 2.12

* #181 CreateTask can't sync fks
* #182 Extract FK resolving into a separate stage for create and create-or-update tasks

## Release 2.11

* #179 Upgrade to DFLib 0.8 

## Release 2.10

* #178 Unreliable FK detection resulting in "Object is not a DataObject" exception

## Release 2.9

* #150 Support for XML namespaces in XPath-based extractor 
* Multi-value extractor properties (implemented as part of #150)
* #175 Upgrade Cayenne to 4.0.2

## Release 2.8

* #159 Execution.java toString is reporting incorrect start time
* #170 "Connector.shutdown" default noop implementation 
* #171 Can't match by binary DbAttribute
* #172 Reorganize LmTask for easier custom implementation 
* #173 Pass parameters down from extractor to StreamConnector
* #174 Upgrade to Cayenne 4.0.1


## Release 2.7

* #160 Upgrade Jackson to 2.9.5
* #162 Remove API deprecated <= v.2.4
* #163 Replace source Row object with just Object[] 
* #164 Switch segments data structures to DataFrame 
* #165 Upgrade Jackson to 2.9.8
* #167 Update Jackson to 2.10.0.pr1
* #168 Cayenne 4.1 binary compatibility

## Release 2.6

* #137 CreateTask for data migrations
* #145 Batch-fault and/or cache target to-one relationships (performance)
* #146 Cayenne 4.0.RC1 is out... upgrading
* #149 Both new and updated targets must be merged during the "merge" stage
* #155 Upgrade to Cayenne 4.0 final

## Release 2.5

* #143 The use of String.replace(..) in PathNormalizer causes massive Pattern creation

## Release 2.4

* #101 Create a URLExtractorModelLoader
* #132 @AfterTargetsMapped listeners are not invoked in CreateOrUpdate tasks
* #134 ValueConverter for enums
* #135 Refactoring JdbcNormalizer to ValueConverter
* #138 Split IExtractorModelLoader into ResourceResolver and ExtractorModelParser
* #139 Deprecate implicit .xml extension
* #140 Upgrade to Cayenne 4.0.B2

## Release 2.3

* #122 Respect original types of JSON source row attributes
* #123 String normalizer
* #128 Support java.math big numeric types in Integer and Long normalizers
* #129 (2.x) Upgrade to Cayenne 4.0.B1

## Release 2.2

* #113 Upgrade Cayenne to 4.0.M4 to fix overlapping transactions
* #114 Targets mapped event before merging update
* #116 tokenManager is null in DefaultDeleteBuilder
* #117 JdbcNormalizer for java.time classes
* #120 Multiple connectors for a single extractor
* #121 upgrade to Cayenne 4.0.M5

## Release 2.1

* #112 API for external access to DI-managed entities
* #110 Relocate the schema to linkmove.io

## Release 2.0

* #71  DataSourceConnector: Out-of-the-box Java 8 support
* #104 Boolean JDBC normalizer fails to normalize a java.lang.Boolean 
* #106 Listener for the after targets committed stage
* #107 Switch to Java 8
* #108 Change license from BSD to Apache 2.0
* #109 Switch version from 1.8 to 2.0

## Release 1.7

* #72 Conflict between source and target DataSources
* #78 Split link-move into a multi-module Maven project, with specialized extractors in submodules
* #83 Support untyped predicates in JSONPath queries
* #84 BigIntNormalizer must handle String values
* #86 XmlExtractor should treat attribute's source name as XPath expression
* #87 JsonExtractor should treat attribute's source name as JsonPath expression
* #88 JdbcNormalizer for SQL Integer
* #89 Implement parent access in JsonPath
* #90 Include deleted count into report
* #91 Similar decimal values cause db update
* #92 Use target's Java type to normalize sources
* #93 Boolean normalizer (sync integers to booleans without updates)
* #94 Do not create TargetToOnePropertyWriter for the master side of a to-one relationship
* #95 Switch to Commons CSV parser
* #97 Split CSV module out of LM core
* #99 Logging API cleanup

## Release 1.6

* #35 Long / Integer values in HashMap keys
* #55 DeleteTask - a new callback between getting source keys and matching them against targets
* #64 Allow mapping source columns to non-persistent target properties
* #73 Reloadable extractors do not reload
* #75 JSON extractor
* #79 Upgrade to Cayenne 4.0.M3
* #80 Start making releases to Maven central
* #81 Remove methods deprecated since 1.4 or earlier

## Release 1.5

* #56 Support for non printable delimiter characters in CSV extractor
* #59 Transient target path support
* #61 Delete task @AfterTargetsMapped listener is missing

## Release 1.4

* #1 XML extractor support
* #36 CSV extractors
* #41 Automatic mapping of row attributes
* #42 Multi-extractor configs
* #43 Deprecate/remove explicit relationship mapping from CreateOrUpdateBuilder
* #44 Straighten mapping by ID
* #46 Cayenne upgrade to 4.0.M3.debfa94
* #49 Matchers must handle expression invariants in Rows
* #50 XML Schema for extractor configs
* #51 Normalize 'sources' map keys
* #52 Descriptor versioning
* #53 FileExtractorModelLoader - a file-based IExtractorModelLoader
* #54 Rename LinkETL to LinkMove

## Release 1.3 2015-04-09

* #23 Support for "delete" of targets missing in the source
* #25 Parameterized queries for Extractors
* #26 Refactoring ETL stack to expose processing chain in the object design
* #27 Upgrade to official Cayenne 4.0.M2
* #28 Refactor TaskBuilder to be able to split it into specialized builders 
* #30 Execution refactoring: split stats in a separate class
* #31 Execution: support for a map of arbitrary 'attributes'
* #32 Task stage listeners
* #33 SourceKeysTask: a task for extraction of source keys
* #34 Stop supporting positional parameters in Extractor templates

## Release 1.2 2014-12-08

* #24 Upgrade Cayenne dependency to 4.0.M2.1ab1caa

## Release 1.1 2014-10-23

* #8 'MatchingTaskBuilder.matchBy' to take Property<?> instead of String
* #9 Matcher refactoring
* #10 KeyBuilder refactoring - renaming to KeyMapAdapter
* #11 Rename MatchingTaskBuilder.matchByPrimaryKey to matchById
* #13 EtlAdapter to package extensions to LinkETL
* #14 Rename "transform" package to "load"
* #15 Rename Matchers to Mappers
* #16 ClasspathExtractorConfigLoader incorrectly resolves resource URLs
* #17 Disallow ID updates for auto-generated IDs
* #18 JdbcExtractorFactory - trim SQL and other properties comping from XML
* #19 Task update count includes related objects
* #20 ETL fails to reset non-null properties to NULL on update
* #21 A default IConnectorFactory using target DataSource for source
* #22 NPE on syncing nullable FK 

## Release 1.0 2014-08-13

[open sourcing LinkETL]


