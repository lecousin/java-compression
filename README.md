# lecousin.net - Java compression framework

This library provides different compression and decompression formats.

Other libraries already exist to do compression and decompression with those formats, but
this library provides implementation using the
[net.lecousin.core]("https://github.com/lecousin/java-framework-core" "java-framework-core") library
taking advantage of multi-threading and more advanced IO model. 

So far only few compression methods are implemented, more will come...

## Supported compression methods

 * deflate and gzip: it uses the [java.util.zip](https://docs.oracle.com/javase/8/docs/api/java/util/zip/package-summary.html) package, wrapping them into cpu tasks to provide asynchronous functionalities
 * mszip: format used by CAB file format, consisting in 32KB blocks of deflate compressed data
 * lzma: taken from [XZ](https://tukaani.org/xz/java.html) library, adapted to provide asynchronous functionalities
 

## Build status

### Current version - branch master

[![Maven Central](https://img.shields.io/maven-central/v/net.lecousin.compression/parent-pom.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.lecousin.compression%22)
![build status](https://travis-ci.org/lecousin/java-compression.svg?branch=master "Build Status")
![build status](https://ci.appveyor.com/api/projects/status/github/lecousin/java-compression?branch=master&svg=true "Build Status")
[![Codecov](https://codecov.io/gh/lecousin/java-compression/graph/badge.svg)](https://codecov.io/gh/lecousin/java-compression/branch/master)

Modules:
 * deflate [![Javadoc](https://img.shields.io/badge/javadoc-0.1.6-brightgreen.svg)](https://www.javadoc.io/doc/net.lecousin.compression/deflate/0.1.6)
 * gzip [![Javadoc](https://img.shields.io/badge/javadoc-0.1.6-brightgreen.svg)](https://www.javadoc.io/doc/net.lecousin.compression/gzip/0.1.6)
 * mszip [![Javadoc](https://img.shields.io/badge/javadoc-0.1.6-brightgreen.svg)](https://www.javadoc.io/doc/net.lecousin.compression/mszip/0.1.6)

### Next minor release - branch 0.1   

![build status](https://travis-ci.org/lecousin/java-compression.svg?branch=0.1 "Build Status")
![build status](https://ci.appveyor.com/api/projects/status/github/lecousin/java-compression?branch=0.1&svg=true "Build Status")
[![Codecov](https://codecov.io/gh/lecousin/java-compression/branch/0.1/graph/badge.svg)](https://codecov.io/gh/lecousin/java-compression/branch/0.1)
