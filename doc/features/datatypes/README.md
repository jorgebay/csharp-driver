# CQL data types to C# types

When retrieving the value of a column from a Row object, you use a getter based on the type of the column.

## C# classes to CQL data types

CQL3 data type|C# type
---|---
ascii|string (Encoding.ASCII)
bigint|long
blob|byte[]
boolean|bool
counter|long
custom|Encoding.UTF8 string
date|LocalDate
decimal|decimal
double|double
float|float
inet|IPEndPoint
int|int
list|IEnumerable&lt;T&gt;
map|IDictionary&lt;K, V&gt;
set|IEnumerable&lt;T&gt;
smallint|short
text|Encoding.UTF8 string
time|LocalTime
timestamp|System.DateTimeOffset
timeuuid|System.Guid
tinyint|sbyte
uuid|System.Guid
varchar|Encoding.UTF8 string
varint|System.Numerics.BigInteger