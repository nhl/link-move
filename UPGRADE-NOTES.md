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
