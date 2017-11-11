# lecousin.net - Java compression framework

This library provides different compression and decompression formats.

Other libraries already exist to do compression and decompression with those formats, but
this library provides implementation using the
[net.lecousin.core]("https://github.com/lecousin/java-framework-core" "java-framework-core") library
taking advantage of multi-threading and more advanced IO model. 

So far only few compression methods are implemented, more will come...

## Build status

Current version: 0.1.1

Master: ![build status](https://travis-ci.org/lecousin/java-compression.svg?branch=master "Build Status")

Branch 0.1: ![build status](https://travis-ci.org/lecousin/java-compression.svg?branch=0.1 "Build Status")

Modules:
 * deflate
   [Javadoc](https://www.javadoc.io/doc/net.lecousin.compression/deflate/0.1.1 "Javadoc")
   [Maven Central Repository](http://search.maven.org/#artifactdetails%7Cnet.lecousin.compression%7Cdeflate%7C0.1.1%7Cjar "Maven")
 * gzip
   [Javadoc](https://www.javadoc.io/doc/net.lecousin.compression/gzip/0.1.1 "Javadoc")
   [Maven Central Repository](http://search.maven.org/#artifactdetails%7Cnet.lecousin.compression%7Cgzip%7C0.1.1%7Cjar "Maven")
 * mszip
   [Javadoc](https://www.javadoc.io/doc/net.lecousin.compression/mszip/0.1.1 "Javadoc")
   [Maven Central Repository](http://search.maven.org/#artifactdetails%7Cnet.lecousin.compression%7Cmszip%7C0.1.1%7Cjar "Maven")
   
