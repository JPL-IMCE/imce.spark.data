# Prototyping SPARK-based analysis of IMCE OMF Schema data

[![Build Status](https://travis-ci.org/JPL-IMCE/imce.spark.data.svg?branch=master)](https://travis-ci.org/JPL-IMCE/imce.spark.data)

This is a prototype to explore SPARK idioms for loading & analyzing OMF Schema tabular data.

## Running

```shell
sbt "run gov.nasa.jpl.imce.spark.data.Example1 <absolute path to OMF Schema.json.zip>"
```

or:

```shell
sbt 
imce.spark.data(master)> run gov.nasa.jpl.imce.spark.data.Example1 <absolute path to OMF Schema.json.zip>
```