# OMQ: An IoT data stream query engine on oneM2M-based middleware

## Introduction

In a common IoT scenario, there are several team of developers involved in several layers of abstraction. 
One team of developer focus on installing, configuring and programming things such as sensors, actuators and other appliances in several areas. Another team of developer focus on gathering data from many of these sensors to be integrated into a centralized system (cloud) and perform several data analytics tasks such as sensor fusion, aggregation, batch processing and produce some meaningful results. 
Also don't forget there are another team who focuses building end-user application in a form of mobile apps, web dashboard, GUI that make uses of processed data from cloud to end-users.
These team together would makes a cohesive well-integrated IoT system that serve a certain beneficial purposes.

With so many team of developers from variety of backgrounds involved to build an IoT system, each developer team have to agree with certain degree of standard and protocols. 
The device configurator should send its sensor data in a compatible format that the cloud infrastructure could agree with. 
The cloud architecture should provide a data processing result that suits the need of end-user application use-cases.
However, this kind of standardization is in contrast with the heterogenous nature of IoT.
Current IoT systems consists of many fragmented and less-interoperable standards that makes integrating many different type of IoT systems into a bigger ecosystem is still hard to achieve.
Every big companies builds their own IoT ecosystem that are somewhat less compatible with other systems.
Unlike the internet system which has been standardized in the form of TCP/IP, the current IoT systems still consists of several varities network protocols, middlewares and data formats.

With such situation, there has been several standardized efforts that promises to embrace heterogenity rather than forcing them to conform single standard.
One of such standard, called oneM2M is being developed by a parternship of several telecom standards defining organization with an objective to minimize M2M/IoT service layer standard market fragmentation by consolidating currently isolated standards activities and jointly developing global specifications.
The standards currently has been implemented in several middlewares, such as: Eclipse OM2M, Mobius, IoTDayLight, etc.
Unline other similar "standardization" efforts, oneM2M reuses several existing standard and support internetworking with another proprietary standard through a proxy mechanism which makes it possible to even integrating a legacy Wireless Sensor Networks (WSN) system into oneM2M.

The OMQ system provides a stream query processing system on top of oneM2M-standardized IoT system.
It provides an ability for IoT developer from either cloud/infrastructure or end-user application to define what IoT data they want to retrieve (what type of sensor, which sensor) AND how to process (aggregate/transform/filter/join) them into a single JSON-based query language.
Several examples of such query including:

  * Notify body tempeature value in wearable worn by one particular worker if it goes beyond a given treshold,
  * Notify if water dam level is beyond treshold,
  * Track position of one or several worker from their wearable devices,
  * Get mean value of several sensors located in same location,
  * Aggregate vehicle count of road sensor from several road every hour

After a user sends a query into the OMQ system, it will perform data retrieval from several oneM2M-enabled IoT nodes (such as ASN/ADN/Sensor Node/Gateway/etc), perform a edge processing when it possible, and join the data into the infrastructure node to be delivered back to user continously until a user stop the query.
The OMQ presents several unique features that are not exist currently:

  * It provides a way to perform BOTH data source searching and data processing in a single query. A user, for instance, can define several sensor data sources by where it is located and its sensor type. Then, they can also define how the data should be processed. OMQ will perform both data retrieval and processing on behalf of user. This is contrast with other stream processing system where a user are expected to feed the data first into the system to make it query-able.
  * It provide more control for a end-user  application developer to make uses an IoT data with dynamically-changing use cases without involving many changes from infrastructure/sensor side. Previously, an application developer only able to make uses of processed data that already programmed by a cloud developer and have to limit their ability to do more.
  * It supports a more user-friendly ad-hoc query through JSON-based query definition. Users are able to perform query and data processing without any programming language
  * It supports processing hundreds/thousands of query simultaneously. Many data processing system are not geared towards doing many different type of processing in and out at the same time.
  * It supports a edge-analytics processing. Most of current IoT system are focused on centralized cloud-based processing where all data are forwarded and processed in a cloud infrastructure. Edge-analytics leverages IoT/sensor nodes by enabling them to perform some processing to reduce amount of data sent through network.

The OMQ, of course, have several limitations.
For once, It supports a limited set of processing operator such as simple aggregates, transformation, filter and join. 
It is not a solution for all data processing in the IoT system. 
OMQ might still have to be paired with other big-data technologies such as Hadoop/Spark to perform a more complex processing.
It is also currently limited to monitoring query, although in the future it will be developed to also supports actuation queries.
The query is still limited to a pre-determined structures containing several operators and single join.

## Installation

OMQ consists of two different Query Engine (QE): Main QE are installed inside the infrastructure node and acts as query interface for an application. Local QE are installed inside IoT/Sensor node and acts as local edge-analytics processors.

OMQ requires a oneM2M-based middleware are already deployed in either infrastructure/IoT node.
At this moment, OMQ only supports a Mobius (for infrastructure node) and &Cube (for IoT node/gateway) middleware provided by IOTOCEAN (http://developers.iotocean.org/).
Furthermore, OMQ also requires a AMQP-based Messaqe Queue server that serves as data retriever from CSE to QE.

### Note on container metadata structure

At this moment, Mobius and &Cube does not support preserving additional metadata information in a resource. Therefore, to make each container be able to be queried using its metadata information, each container in an AE should be paired with another container called "xx<containerName>" that consists of a single contentInstance containing metadata in simple key-value JSON object.
An AE can be also preserved with metadata information by adding a container called "metadata" inside the AE using the same key-value JSON object.

For example: Suppose a CSE in one IoT node consists of one AE called "TempMonitoringAE" with one container called "temperature". We can add some metadata information for temperature container such as location and type by adding another container called "xxtemperature" inside "TempMonitoringAE" and insert a contentInstance in the new container with value `{"location": "site1", "type": "temperature"}`. This way, user will able to search the specific sensor with the same key-value structure.

In the future, the QE should supports a standardized metadata definition through semanticDescriptor supported by oneM2M standard version 2.0.


### Local QE Installation

To install Local QE inside a Linux-based IoT Node, follow these steps:

  - Make sure &Cube CSE software and its supporting AE are already installed and running.
  - Install RabbitMQ and configure so it can be accessed from remote node.
  - Install JDK and Node.js.
  - Open terminal and go to the repo's main directory.
  - Perform a compilation of the QE software by running this command: `mvn package`.
  - If compilation is success, copy the QE jar file (from target/queryengine-1.0-SNAPSHOT.jar) into a new directory with a new configuration file `config_edge.json`.
  - Go to the new directory.
  - Edit `config_edge.json` by modifying some value such as:
    - `cse_address`: The main HTTP endpoint of IoT node's CSE.
    - `cse_root_name`: The main cse entrypoint.
    - `main_cse_root_name`: The infrastructure CSE remote CSE entry, make sure &Cube are already configured and registered inside IN-CSE.
    - `qe_address`: The local QE IP address, which is the node IP (dont use localhost address)
    - `main_qe_address`: The IP address of main QE, which is the same as IN's IP.
  - Before running the QE, you have to add a new AE inside IoT node's CSE with following information: `{"rn": "query-engine", "rr": true, "poa": ["http://127.0.0.1:1111"]}`.
  - To run QE, do `java -cp queryengine-1.0-SNAPSHOT onem2m.queryengine.localprocessorng.App`.
  - Also at the same time, you have to run the cse-to-qe script that will forward CSE data to the QE. To run the script, enter qe-to-cse directory inside main repo's, perform `npm install; node data-forwarder.js`. 
  - Make sure that the query-engine AE are already subscribing all containers from every AE.

### Main QE Installation

To install Main QE in Linux-based Infrastructure Node (IN), follow these steps:

  - Make sure Mobius CSE software and its supporting AE are already installed and running.
  - Install RabbitMQ and configure so it can be accessed from remote node.
  - Install JDK and Node.js.
  - Open terminal and go to the repo's main directory.
  - Perform a compilation of the QE software by running this command: `mvn package`.
  - If compilation is success, copy the QE jar file (from `target/queryengine-1.0-SNAPSHOT.jar`) into a new directory with a new configuration file `config.json`.
  - Go to the new directory.
  - Edit `config.json` by modifying some value such as:
    - `cse_address`: The main HTTP endpoint of IN's CSE.
    - `cse_root_name`: The main cse entrypoint.
    - `local_qes`: Mention all remote CSE node name that are installed with local QE.
    - `qe_address`: The local QE IP address, which is the node IP (dont use localhost address).
  - To run QE, do `java -cp queryengine-1.0-SNAPSHOT onem2m.queryengine.processorng.App`.

