## JSON Extractor

JSON extractor supports an informal query language [JSONPath](http://goessner.net/articles/JsonPath/), which bears strong resemblance to XPath (used for querying XML documents). The JSONPath query should be specified in `extractor.json.path` property of the extractor configuration.

### Supported JSONPath syntax

|Element|Description|
|---|---|
|$|Root node|
|@|Current node|
|. or []|Child/subscript operator (list indices always start by 0)|
|..|Recursive descent operator|
|*|All elements in a list or all children of an object regardless of their names|
|' or "|Designates a child's name inside a subscript operator or a literal string value inside a script expression|
|()|Script expression. Can be used inside a subscript operator or inside another script expression|
|?()|Filter with a script expression acting as a predicate|

#### Recursive descent

Recursive descent operator produces a collection of all nodes in the current node's subtree. Subsequent query segment (if present) is applied to each node in this "aggregate" collection.

E.g. for the root JSON document:
```
{
  store: {
    book: [
      {
        title: ...,
        isbn: ...,
        readers: [...]
      },
      {
        ...,
        readers: [...]
      },
      {
        ...,
        readers: [...]
      }
    ]
  }
}
```

Recursive descent can be used to select all readers:

`$..readers[*]`
(note how the wildcard `*` is used to combine three `readers` arrays into one "flattened" collection)

### Script expressions

Script is an expression that evaluates to some value (or a collection of values). It has a slightly different syntax. Simplest expression is something like this:

`1 == 1`, which evaluates to `true`.

Either side of the expression can be one of:
- JSONPath query (either root, `$`, or local, `@`)
- script
- scalar value (a string, numeric or boolean literal)

There is a number of built-in operators like `&&` and `||`, a few equality and comparison operators and even a regex match operator, using which you can build pretty complex expressions (if you need so):

`@.name =~ 'ab.*' && @.age >= $.constants.minAge`

Built-in functions (binary operators) that can be used inside a script expression:

|Literal name|Description|
|---|---|
|&&, \|\||Logical AND and OR operators. Both sides must evaluate to a boolean value and can be either one of: true/false literal, query or a nested expression)|
|==, !=|Equality operators. Both sides must have the same type, one of: boolean, string, number|
|<, <=, >, >=|Comparison operators. Both sides must have the same comparable type, one of: boolean, string, number|
|=~|String match operator. Both sides must evaluate to a string value with the right-hand side being a Java regex|

#### Dynamic properties

You can use scripts inside a subscript operator to dynamically resolve a child's name or a list index:
$.store..readers[(_expression returning a list index_)]

In this case the script must return a scalar value (string or numeric).

#### Predicates

The most important use-case for scripts is filters. Using them you may filter a collection of nodes to select only those ones that satisfy the predicate expression. Semantics for predicate expressions are a bit subtle but simple:
- if the evaluation of the expression produces a `boolean` value, then only nodes for which this value is `true` are added to the filtered collection
- if the evaluation result is not a `boolean`, then it just needs to have _some_ value (e.g. be not "null" and not "empty") for the node to be added to the result; in some sense such filter just checks that _something **exists**_

`$.store.book[*].readers[?(@.age >= 30)]`
All readers aged 30+

`$.store.book[?(@.readers)]`
All books that have readers

`$.store.book[?(@.readers[?(@.age >= 30)])]`
All books that have readers aged 30+
