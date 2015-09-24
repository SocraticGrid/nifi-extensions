# README #

This repository contains a collection of NiFi extensions.

[More information about NiFi can be found here](https://nifi.incubator.apache.org/)

## How to create a new Processor in NiFi? ##

NiFi allows custom extensions using the concept of a .nar file. The easiest way to create an extension project is by using NiFi's nar archetype.

### Create an instance of a nar project ###

`mvn -DarchetypeGroupId=org.apache.nifi -DarchetypeArtifactId=nifi-processor-bundle-archetype -DarchetypeVersion=0.0.2-incubating-SNAPSHOT -DarchetypeRepository=local -DgroupId=com.cognitivemedicine -DartifactId=my-extension -Dversion=0.1-SNAPSHOT -Dpackage=com.cognitivemedicine.sample -Dbasedir=/tmp -DartifactBaseName=sample -DnifiVersion=0.0.2-incubating-SNAPSHOT -Darchetype.interactive=false --batch-mode --update-snapshots archetype:generate`

Make sure you customize all the necessary parameters in the previous command line before you actually executes it.

This command will a maven project with 2 sub-modules: 
1. nifi-sample-processors: this is where the implementation of custom processors needs to be placed.
2. nifi-sampe-nar: this module depends on the latter and is used to package it in a .nar file.

### Copy the generated .nar file into NiFi ###

Once you have implemented your own processor/s you need to copy the generated .nar file under `nifi-sample-nar/target` into `$NIFI_HOME/lib`.
