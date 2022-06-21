# Template Repository for a Java Tool (CLI)

This template uses the Apache Maven build system and assumes a requirement to connect to an Oracle database.

### System Prerequisites

- Java 11+
- Maven 3+

### Local Prerequisites

Because it depends on fgputil-db for easy DB access, you must provide credentials in your build
environment to access VEuPathDB's package repository (Github Packages).  The required environment
variables are:
```
GITHUB_USERNAME
GITHUB_TOKEN
```

### Building the Project

A convenience script (`./build.sh`) is provided that will check the surrounding environment and
package the project.

Note the first time this project builds (especially with an empty local maven repository), it may
take a few minutes as it downloads dependencies.  Subsequent builds will be much faster.

A successful build will result in an executable uber jar (jar with dependencies) that will run
the Java main() method inside.  To change which main method is used, you must open pom.xml and
change the fully-qualified class name in the `java.main.class` property.

### Executing the Tool

A convenience script (`./bin/sampleTool`) is provided which will find and execute the main class in
the executable jar (passing along any arguments).  You may wish to write additional scripts that
call various options on the main class or even call a main method in a different class.
