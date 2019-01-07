## Upgrading to 2.7

### Segment data structures changed to DataFrame [#164](https://github.com/nhl/link-move/issues/164)

All the operation segment data structures have changed from Lists and Maps to
[YADF DataFrames](https://github.com/nhl/yadf). This unifies the data structures
and makes them easier to process and debug, but will require adapting custom
listeners to the new API. Watch for listener compilation errors, and adjust
them as needed. Note that the names of the standard DataFrame columns
are declared as String constants in the corresponding segments. E.g.
`CreateOrUpdateSegment.KEY_COLUMN`, etc.

## Upgrading to 2.6

### @AfterTargetsMapped listener won't see the state of new objects [#XXX](https://github.com/nhl/link-move/issues/XXX)

Since new objects are now populated with values during "merge" stage (as they should) instead of "map" stage, users
should review their use of @AfterTargetsMapped listener to ensure it (a) does not depend on new objects having their properties set
and (b) does not access segment's "merged" collection (as it will be null after the map stage), using the new "mapped"
collection. Notice that "mapped" may potentially contain "phantom" updates that are later weeded out during the "merge" stage.
Potentially @AfterTargetsMerged listener may also be affected, though it is less clear how.

## Upgrading to 2.4

### IExtractorModelLoader is replaced by ResourceResolver/IExtractorModelParser [#138](https://github.com/nhl/link-move/issues/138)

`IExtractorModelLoader` did two things - loading from XML and parsing the extractor model. With a bunch of subclasses for 
different XML sources this became confusing. So we need to split this into two separate interfaces: `ResourceResolver` and
`IExtractorModelParser`. This is a breaking change if you used custom model loaders. Very likely you will need to modify 
your custom loader to implement `ResourceResolver` and use `LmRuntimeBuilder.extractorResolver(..)` to load it in the stack.

## Upgrading to 2.1

### Requires XSD location update [#110](https://github.com/nhl/link-move/issues/110)

Change schema location in all XML extractors from http://nhl.github.io/link-move/xsd/extractor_config_2.xsd to http://linkmove.io/xsd/extractor_config_2.xsd

## Upgrading to 2.0

### Requires Java 8 [#109](https://github.com/nhl/link-move/issues/109)

2.0 release requires Java 8. 

## Upgrading to 1.6

### Remove methods deprecated since 1.4 or earlier [#81](https://github.com/nhl/link-move/issues/81)

All previously deprecated APIs are removed. Most noticeable one is LoadListener. Use 
annotated listeners instead.

## Upgrading to 1.4

### Rename LinkETL to LinkMove [#54](https://github.com/nhl/link-etl/issues/54)

LinkETL project was renamed to LinkMove. This means the following to the end users:

* pom.xml should now include com.nhl.link.move:link-move artifact.
* com.nhl.link.etl package is now com.nhl.link.etl.move. Classes starting with Etl are now starting with Lm 
(e.g. EtlRuntime -> LmRuntime). So you will need to do renaming in your code. 

### Straighten mapping by ID [#44](https://github.com/nhl/link-etl/issues/44)

XML descriptors that map ID columns MUST BE CHANGED to use Cayenne db: expression syntax:

```XML
<attribute>
    <type>java.lang.Integer</type>
    <source>SOURCE_ID</source>
    <target>db:TARGET_ID</target>
</attribute>
```

Your IDE would show an error for 'matchById(String)' method, giving a hint that 
you need to review the descriptors and switch ID attributes to "db:" expressions. 
But in other cases there may be no such hints.

### Cayenne upgrade to 4.0.M3.debfa94 [#46](https://github.com/nhl/link-etl/issues/46)

1.4 requires a version of Cayenne 4.0.M3.debfa94 or newer for its core functionality. 
Unofficial builds of Cayenne are available from ObjectStyle.org repository:
http://maven.objectstyle.org/nexus/content/repositories/cayenne-unofficial/	

### Additive 'matchBy' in CreateOrUpdateBuilder/DeleteBuilder/MapperBuilder [#48](https://github.com/nhl/link-etl/issues/48)

If you have more than one 'matchBy' in your builder chain, you will need to rewrite it, 
as the task will now work differently.

###  Normalize 'sources' map keys [#51](https://github.com/nhl/link-etl/issues/51)

"sources" is a ```List<Map<String, Object>>``` created from ```List<Row>``` during the first stage 
of most transformation flows. Currently the keys in the sources map can be an unpredictable mix of 
obj: and db: expressions, defined by the XML mapping, automatic mapping of source column names, etc. 

This task will ensure that "sources" will be uniformly mapped by "db:column_name" expression, 
no matter how the original extractor mapping was created.

If you had listeners that read or write from/to the "sources" collection, you will need to take 
the new rules into account. 
