# Avro-Search
A simple tool to perform case-sensitive searches of AVRO files for the specified key/value pair. If an AVRO record is found with the specified key and value, it's written to System.out as JSON. Otherwise the user is told if the record is not found.

# Usage

java -jar avro-search {avro-file} {key} {value}

  - avro-file: The full path to the AVRO file to search.
  - key: The record header to find. Nested records can be denoted using a period.
  - value: The value to find.
