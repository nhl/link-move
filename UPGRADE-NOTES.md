## Upgrading to 1.4

### [Straighten mapping by ID #44](https://github.com/nhl/link-etl/issues/44)

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

### [Cayenne upgrade to 4.0.M3.debfa94 #46](https://github.com/nhl/link-etl/issues/46)

1.4 requires a version of Cayenne 4.0.M3.debfa94 or newer for its core functionality. 
Unofficial builds of Cayenne are available from ObjectStyle.org repository:
http://maven.objectstyle.org/nexus/content/repositories/cayenne-unofficial/	

### [Additive 'matchBy' in CreateOrUpdateBuilder/DeleteBuilder/MapperBuilder #48](https://github.com/nhl/link-etl/issues/48)

If you have more than one 'matchBy' in your builder chain, you will need to rewrite it, 
as the task will now work differently.