###### Build status for branch:
 - Master:
   - [![build status](https://gitlab.prtech.mk/prtech/svarog/badges/master/build.svg)](https://github.com/PerunTech/svarog/commits/master)
 - Staging:
   - [![build status](https://gitlab.prtech.mk/prtech/svarog/badges/staging/build.svg)](https://github.com/PerunTech/svarog/commits/staging)
 - Dev:
   - [![build status](https://gitlab.prtech.mk/prtech/svarog/badges/dev/build.svg)](https://github.com/PerunTech/svarog/commits/dev)

### Git; Branch and Stability Info
Source control is `Git` exclusive:

* The `master` branch is updated only from the current state of the `staging` branch
* The `staging` branch must only be updated with commits from the `dev` branch
* The `dev` branch contains all the latest additions to the project
* All larger feature updates must be developed in their own branch and later merged into `dev`


### Changelog for V3

All basic Svarog structures are now moved to a different library called svarog-interfaces. This should be on the build path

The DbDataObject constructor which takes an object name is no more available. The object name must be resolved before the contructor is invoked.
* Old: `new DbDataObject("LABELS");`
* New: `new DbDataObject(SvCore.getDbtByName("LABELS").getObject_id());`

The method `DbDataArray.getDistinctValuesPerColumns` no more accepts a `SvReader` as parameter to resolve field names. The field names must be resolved previously.

* Old: `DbDataArray1.getDistinctValuesPerColumns(columnsSpecified, svReader1);`
* New: `DbDataArray fieldsPerObjectType = svr.getObjectsByParentId(objectTypeId1, svCONST.OBJECT_TYPE_FIELD, null, 0, 0);`
	`DbDataArray1.getDistinctValuesPerColumns(columnsSpecified, fieldsPerObjectType);`


### Bootstrap of V3

##Prerequisites
1. Install latest version of Apache Maven. On linux just do: sudo apt install maven
2. Install git (standalone or eclipse module)
3. Pull the following dependencies from github:
* 	`svarog-jts-io (https://github.com/PerunTech/svarog-jts-io)`
* 	`svarog-interfaces (https://github.com/PerunTech/svarog-interfaces)`

4. On each of the projects (exactly in the same order as they appear on the list above) go into the project directory and run. 
	
*  `$ mvn install`

5. Pull the latest version of svarog and configure your properties file.
	
	5.1: To generate JSON files for installation run maven with java@json target. According to your properties configuration you need to set the correct profile. If oracle then "-P Oracle". If Posgres then "-P PostgreSQL"
	$ mvn exec:java@json -P PostgreSQL
	
	5.2 To install fresh svarog the target is install
	$ mvn exec:java@install -P PostgreSQL 
	
	5.3 To install fresh svarog with drop of the schema (for Postgres only)
	$ mvn exec:java@install-drop -P PostgreSQL
	
	5.4 To upgrade svarog
	$ mvn exec:java@upgrade-force -P PostgreSQL
	
	5.5 To run unit tests
	$ mvn test -P PostgreSQL
		
	5.6 To install the svarog jar in the maven repository
	$mvn install
	
	5.7 Unpack the default osgi-bundles-default.zip into the osgi-bundles folder (to have some bundles to play with)
	
	5.8 Pull the triglav-core project from gitlab, package as jar and place in the osgi-bunlde dir to provide basic proof of concept how to use Svarog v3. In order to build triglav-core sample you need to install svarog library first 
	$mvn install -DskipTests
	
	5.8 To run the svarog OSGI container 
	$mvn exec:java@osgi -P PostgreSQL
	
	
6. Unzip the osgi-bundles-defaults.zip (Download from https://github.com/PerunTech/svarog/blob/83fa485dc0f0141a3b18de1dea609865ae0ab671/osgi-bundles-default.zip) archive into the directory osgi-bundles (or the directory you configured in your svarog.properties file) and restart the svarog osgi container. Now you can see the standard felix admin console at http://localhost:8080/system/console.