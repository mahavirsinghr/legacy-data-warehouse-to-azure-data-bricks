# legacy-data-warehouse-to-azure-data-bricks

Data Quality Improvement Framework - Thought Exercise
 
1.	Scenario: You are a seasoned Azure Data Architect, and your prospective client is a leading organization facing persistent data quality issues in both their 
a.	Legacy Data warehouse environment and
b.	Their Azure Databricks environment.

2.	Prepare a technical proposal that outlines 
a.	a comprehensive framework to enhance the quality of their data. 
b.	This proposal will be presented to the VP of IT for their consideration.

3.	Assignment Instructions:
a.	Assumption: You can feel free to take assumption related to Data quality based on your experience.
b.	Proposed Framework: Present a wholistic approach for a Data Quality Improvement Framework.
c.	You can submit the proposal in a format of your choice, such as Word, Excel, PowerPoint, or any other preferred format.

4.	Evaluation Criteria: Your assignment will be evaluated based on the 
a.	clarity of your thoughts
b.	the depth of your analysis
c.	the effectiveness of your framework
d.	and your ability to communicate the 
i.	benefits and 
ii.	ROI of the proposed solution.

5.	Feel free to format and structure your assignment in a way that best conveys your 
a.	ideas. 
b.	We look forward to reviewing your proposal in your chosen format.


1.	What Is Data Quality?

a.	Data quality is a key success factor for all organizations. Undetected errors or invalid data can mislead decision-makers, create missed opportunities, and prevent compliance with regulations. According to Gartner, data quality issues cost the average organization $12.9 million every year.

b.	However, ensuring data quality at scale is not an easy task, as it requires a combination of people, processes, and technology to guarantee success. We will explore how Databricks can help with data quality management in analytical data platforms, and how customers can accelerate the implementation of a data quality management framework with Delta Live Tables (DLT).

c.	Data Quality Diagram by Length and Breadth of Data using Databricks

![image](https://github.com/mahavirsinghr/legacy-data-warehouse-to-azure-data-bricks/assets/13980382/e0266f4e-dfb2-4205-8bc6-4284666505be)
 
Principles of Data Quality
2.	Breaking down into 6 dimensional models
a.	Consistency – Data values should not conflict with other values across your data sets 
b.	Accuracy – There should be no errors in your data
c.	Validity – Your data should conform to a certain format
d.	Completeness – There should be no missing data
e.	Timeliness – Your data should be up to date
f.	Uniqueness – There should be no duplicates

3.	Consistency
a.	Data values used by data consumers do not conflict with each other. For instance, the value ‘customer churn’ will return the same result when queried by analysts from table_a or by data scientists from table_b
b.	The correct data is returned to the user/written, regardless of concurrent read or write processes affecting the relevant data objects

4.	When to Use Constraints, Expectations, Quarantining or Violation Flagging

a.	Constraints
i.	Will block the entire load if any constraints are violated
ii.	Simple implementation

b.	Expectations
i.	Uses DLT: higher level of abstraction
ii.	Declarative quality expectations

c.	Quarantine
i.	Keeps job running
ii.	Loads good data into the target table
iii.	Isolates bad data in a quarantine table

d.	Flag Violations
i.	Keeps job running
ii.	Loads all data into the target table
iii.	Tags bad data

5.	facing persistent data quality issues in both their 
a.	Legacy Data warehouse environment and 
i.	Modern data warehouse for small and medium business
1.	Legacy SMB data warehouses might contain several types of data:
a.	This example workload shows several ways that small businesses (SMBs) can modernize legacy data stores and explore big data tools and capabilities, without overextending current budgets and skillsets. These end-to-end Azure data warehousing solutions integrate easily with tools like Azure Machine Learning, Microsoft Power Platform, Microsoft Dynamics, and other Microsoft technologies.
2.	Unstructured data, like documents and graphics
3.	Semi-structured data, such as logs, CSVs, JSON, and XML files
4.	Structured relational data, including databases that use stored procedures for extract-transform-load/extract-load-transform (ETL/ELT) activities
5.	Diagram

![image](https://github.com/mahavirsinghr/legacy-data-warehouse-to-azure-data-bricks/assets/13980382/266bce87-664d-4984-bb94-6236d8623435)
 
ii.	Dataflow
1.	 Azure Data Factory
2.	Azure SQL Database is an intelligent, scalable, relational database service built for the cloud. In this solution, SQL Database holds the enterprise data warehouse and performs ETL/ELT activities that use stored procedures.
3.	Azure Event Hubs is a real-time data streaming platform and event ingestion service. Event Hubs can ingest data from anywhere, and seamlessly integrates with Azure data services.
4.	Azure Stream Analytics is a real-time, serverless analytics service for streaming data. Stream Analytics offers rapid, elastic scalability, enterprise-grade reliability and recovery, and built-in machine learning capabilities.
5.	Azure Machine Learning is a toolset for data science model development and lifecycle management. Machine Learning is one example of the Azure and Microsoft services that can consume fused, processed data from Data Lake Storage Gen2.
b.	Their Azure Databricks environment.
i.	Stream processing with Azure Databricks
1.	This reference architecture shows an end-to-end stream processing pipeline.
a.	This type of pipeline has four stages: 
i.	Ingest
ii.	Process
iii.	Store
iv.	Analysis
v.	reporting. 

2.	For this reference architecture, the pipeline ingests data from two sources,
a.	performs a join on related records from each stream, 
i.	enriches the result
ii.	calculates an average in real time. 
iii.	The results are stored for further analysis.

![image](https://github.com/mahavirsinghr/legacy-data-warehouse-to-azure-data-bricks/assets/13980382/1e3049c4-8102-4c13-aa91-285ba542825c)

ii.	Workflow

1.	The architecture consists of the following components:
a.	Data sources. 
i.	In this architecture, there are two data sources that generate data streams in real time. 
1.	The first stream contains ride information
2.	second contains fare information. 
3.	The reference architecture includes a simulated data generator that reads from a set of static files and pushes the data to Event Hubs. 
4.	The data sources in a real application would be devices installed in the flight takeoffs.

b.	Azure Event Hubs. 
i.	event ingestion service. 
ii.	This architecture uses two event hub instances, 
1.	one for each data source. 
2.	Each data source sends a stream of data to the associated event hub.

c.	Azure Databricks. 
i.	Is an Apache Spark-based analytics platform optimized for the Microsoft Azure cloud services platform. 
ii.	Databricks is used to correlate of the flight ride and fare data, and also to enrich the correlated data with neighborhood data stored in the Databricks file system.

d.	Azure Cosmos DB.
i.	The output of an Azure Databricks job is a series of records, which are written to 
ii.	Azure Cosmos DB for Apache Cassandra is used because it supports time series data modeling.

e.	Azure Databricks Link for Azure Cosmos DB enables you to run near real-time analytics over operational data in Azure Cosmos DB, without any performance or cost impact on your transactional workload, by using the two analytics engines available from your Azure Databricks workspace: SQL Serverless and Spark Pools.

f.	Azure Log Analytics. Application log data collected by Azure Monitor is stored in a Log Analytics workspace. Log Analytics queries can be used to analyze and visualize metrics and inspect log messages to identify issues within the application.

iii.	Scenario Details: A aviation company collects data about each aviation trip. For this scenario, we assume there are two separate devices sending data. The aviation has a meter that sends information about each ride — the duration, distance, and pickup and drop-off locations. A separate device accepts payments from customers and sends data about fares. To spot ridership trends, the flight company wants to calculate the average tip per mile driven, in real time, for each neighbourhood.

iv.	Data ingestion 
To simulate a data source, this reference architecture uses the the Chicago city flight dataset[1]. This dataset contains data about flight trips in Chicago city over a four-year period (2010 – 2013). It contains two types of record: Ride data and fare data. Ride data includes trip duration, trip distance, and pickup and drop-off location. Fare data includes fare, tax, and tip amounts. Common fields in both record types include medallion number, hack license, and vendor ID. Together these three fields uniquely identify a flight plus a pilot. The data is stored in CSV format.

1.	The data generator is a .NET Core application that reads the records and sends them to Azure Event Hubs. The generator sends ride data in JSON format and fare data in CSV format.

2.	Event Hubs uses partitions to segment the data. Partitions allow a consumer to read each partition in parallel. 

a.	When you send data to Event Hubs, 
i.	you can specify the partition key explicitly. 
ii.	Otherwise, records are assigned to partitions in round-robin fashion.

b.	In this scenario, ride data and fare data should end up with the same partition ID for a given flight booking. This enables Databricks to apply a degree of parallelism when it correlates the two streams. A record in partition n of the ride data will match a record in partition n of the fare data.

c.	In the data generator, the common data model for both record types has a PartitionKey property that is the concatenation of Medallion, HackLicense, and VendorId.

d.	
![image](https://github.com/mahavirsinghr/legacy-data-warehouse-to-azure-data-bricks/assets/13980382/6e7497e4-1f0c-4c0f-8a2b-6723e5fb3557)
e.	

public abstract class FlightData
{
    public FlightData()
    {
    }

    [JsonProperty]
    public long Medallion { get; set; }

    [JsonProperty]
    public long HackLicense { get; set; }

    [JsonProperty]
    public string VendorId { get; set; }

    [JsonProperty]
    public DateTimeOffset PickupTime { get; set; }

    [JsonIgnore]
    public string PartitionKey
    {
        get => $"{Medallion}_{HackLicense}_{VendorId}";
    }
This property is used to provide an explicit partition key when sending to Event Hubs:
using (var client = pool.GetObject())
{
    return client.Value.SendAsync(new EventData(Encoding.UTF8.GetBytes(
        t.GetData(dataFormat))), t.PartitionKey);
}

f.	Event Hubs
i.	The throughput capacity of Event Hubs is measured in throughput units. You can autoscale an event hub by enabling auto-inflate, which automatically scales the throughput units based on traffic, up to a configured maximum.

g.	Stream processing
In Azure Databricks, data processing is performed by a job. The job is assigned to and runs on a cluster. The job can either be custom code written in Java, or a Spark notebook.

h.	In this reference architecture, 
i.	the job is a Java archive with classes written in both 
1.	Java
2.	Scala. 
3.	When specifying the Java archive for a Databricks job, the class is specified for 
a.	execution by the Databricks cluster. 
b.	Here, the main method of the com.microsoft.pnp.FlightTakeoffReader class contains the data processing logic.
i.	Reading the stream from the two event hub instances
j.	The data processing logic uses Spark structured streaming to read from the two Azure event hub instances:
val rideEventHubOptions = EventHubsConf(rideEventHubConnectionString)
      .setConsumerGroup(conf.flightRideConsumerGroup())
      .setStartingPosition(EventPosition.fromStartOfStream)
    val rideEvents = spark.readStream
      .format("eventhubs")
      .options(rideEventHubOptions.toMap)
      .load

    val fareEventHubOptions = EventHubsConf(fareEventHubConnectionString)
      .setConsumerGroup(conf.flightFareConsumerGroup())
      .setStartingPosition(EventPosition.fromStartOfStream)
    val fareEvents = spark.readStream
      .format("eventhubs")
      .options(fareEventHubOptions.toMap)
      .load
k.	Enriching the data with the neighbourhood information
The ride data includes the latitude and longitude coordinates of the pickup and drop off locations. While these coordinates are useful, they are not easily consumed for analysis. Therefore, this data is enriched with neighbourhood data that is read from a shapefile.
The shapefile format is binary and not easily parsed, but the GeoTools library provides tools for geospatial data that use the shapefile format. This library is used in the com.microsoft.pnp.GeoFinder class to determine the neighbourhood name based on the pickup and drop off coordinates.
val neighborhoodFinder = (lon: Double, lat: Double) => {
      NeighborhoodFinder.getNeighborhood(lon, lat).get()
    }
Joining the ride and fare data
First the ride and fare data is transformed:
ScalaCopy
    val rides = transformedRides
      .filter(r => {
        if (r.isNullAt(r.fieldIndex("errorMessage"))) {
          true
        }
        else {
          malformedRides.add(1)
          false
        }
      })
      .select(
        $"ride.*",
        to_neighborhood($"ride.pickupLon", $"ride.pickupLat")
          .as("pickupNeighborhood"),
        to_neighborhood($"ride.dropoffLon", $"ride.dropoffLat")
          .as("dropoffNeighborhood")
      )
      .withWatermark("pickupTime", conf.flightRideWatermarkInterval())

    val fares = transformedFares
      .filter(r => {
        if (r.isNullAt(r.fieldIndex("errorMessage"))) {
          true
        }
        else {
          malformedFares.add(1)
          false
        }
      })
      .select(
        $"fare.*",
        $"pickupTime"
      )
      .withWatermark("pickupTime", conf.flightFareWatermarkInterval())
And then the ride data is joined with the fare data:
Scala
val mergedFlightTrip = rides.join(fares, Seq("medallion", "hackLicense", "vendorId", "pickupTime"))
l.	Processing the data and inserting into Azure Cosmos DB
The average fare amount for each neighborhood is calculated for a given time interval:
Scala
val maxAvgFarePerNeighborhood = mergedFlightTrip.selectExpr("medallion", "hackLicense", "vendorId", "pickupTime", "rateCode", "storeAndForwardFlag", "dropoffTime", "passengerCount", "tripTimeInSeconds", "tripDistanceInMiles", "pickupLon", "pickupLat", "dropoffLon", "dropoffLat", "paymentType", "fareAmount", "surcharge", "mtaTax", "tipAmount", "tollsAmount", "totalAmount", "pickupNeighborhood", "dropoffNeighborhood")
      .groupBy(window($"pickupTime", conf.windowInterval()), $"pickupNeighborhood")
      .agg(
        count("*").as("rideCount"),
        sum($"fareAmount").as("totalFareAmount"),
        sum($"tipAmount").as("totalTipAmount"),
        (sum($"fareAmount")/count("*")).as("averageFareAmount"),
        (sum($"tipAmount")/count("*")).as("averageTipAmount")
      )
      .select($"window.start", $"window.end", $"pickupNeighborhood", $"rideCount", $"totalFareAmount", $"totalTipAmount", $"averageFareAmount", $"averageTipAmount")
Which is then inserted into Azure Cosmos DB:
Scala
maxAvgFarePerNeighborhood
      .writeStream
      .queryName("maxAvgFarePerNeighborhood_cassandra_insert")
      .outputMode(OutputMode.Append())
      .foreach(new CassandraSinkForeach(connector))
      .start()
      .awaitTermination()
6.	Considerations
a.	These considerations implement the pillars of the Azure Well-Architected Framework, which is a set of guiding tenets that can be used to improve the quality of a workload. For more information, see Microsoft Azure Well-Architected Framework.
7.	Security
Security provides assurances against deliberate attacks and the abuse of your valuable data and systems. For more information, see Overview of the security pillar.
Access to the Azure Databricks workspace is controlled using the administrator console. The administrator console includes functionality to add users, manage user permissions, and set up single sign-on. Access control for workspaces, clusters, jobs, and tables can also be set through the administrator console.
8.	Managing secrets 
Azure Databricks includes a secret store that is used to store secrets, including connection strings, access keys, user names, and passwords. Secrets within the Azure Databricks secret store are partitioned by scopes:

Bash
databricks secrets create-scope --scope "azure-databricks-job"
Secrets are added at the scope level:
BashCopy
databricks secrets put --scope "azure-databricks-job" --key "flight-ride"
 Note
An Azure Key Vault-backed scope can be used instead of the native Azure Databricks scope. To learn more, see Azure Key Vault-backed scopes.
In code, secrets are accessed via the Azure Databricks secrets utilities.
9.	Monitoring
Azure Databricks is based on Apache Spark, and both use log4j as the standard library for logging. In addition to the default logging provided by Apache Spark, you can implement logging to Azure Log Analytics following the article Monitoring Azure Databricks.
As the com.microsoft.pnp.FlightTakeOffReader class processes ride and fare messages, it's possible that either one may be malformed and therefore not valid. In a production environment, it's important to analyze these malformed messages to identify a problem with the data sources so it can be fixed quickly to prevent data loss. The com.microsoft.pnp.FlightTakeOffReader class registers an Apache Spark Accumulator that keeps track of the number of malformed fare and ride records:
Scala
    @transient val appMetrics = new AppMetrics(spark.sparkContext)
    appMetrics.registerGauge("metrics.malformedrides", AppAccumulators.getRideInstance(spark.sparkContext))
    appMetrics.registerGauge("metrics.malformedfares", AppAccumulators.getFareInstance(spark.sparkContext))
    SparkEnv.get.metricsSystem.registerSource(appMetrics)
Apache Spark uses the Dropwizard library to send metrics, and some of the native Dropwizard metrics fields are incompatible with Azure Log Analytics. Therefore, this reference architecture includes a custom Dropwizard sink and reporter. It formats the metrics in the format expected by Azure Log Analytics. When Apache Spark reports metrics, the custom metrics for the malformed ride and fare data are also sent.
The following are example queries that you can use in your Azure Log Analytics workspace to monitor the execution of the streaming job. The argument ago(1d) in each query will return all records that were generated in the last day, and can be adjusted to view a different time period.
Exceptions logged during stream query execution
KustoCopy
SparkLoggingEvent_CL
| where TimeGenerated > ago(1d)
| where Level == "ERROR"
Accumulation of malformed fare and ride data
KustoCopy
SparkMetric_CL
| where TimeGenerated > ago(1d)
| where name_s contains "metrics.malformedrides"
| project value_d, TimeGenerated, applicationId_s
| render timechart

SparkMetric_CL
| where TimeGenerated > ago(1d)
| where name_s contains "metrics.malformedfares"
| project value_d, TimeGenerated, applicationId_s
| render timechart
Job execution over time
KustoCopy
SparkMetric_CL
| where TimeGenerated > ago(1d)
| where name_s contains "driver.DAGScheduler.job.allJobs"
| project value_d, TimeGenerated, applicationId_s
| render timechart
For more information, see Monitoring Azure Databricks.
10.	DevOps
•	Create separate resource groups for production, development, and test environments. Separate resource groups make it easier to manage deployments, delete test deployments, and assign access rights.
•	Use Azure Resource Manager template to deploy the Azure resources following the infrastructure as Code (IaC) Process. With templates, automating deployments using Azure DevOps Services, or other CI/CD solutions is easier.
•	Put each workload in a separate deployment template and store the resources in source control systems. You can deploy the templates together or individually as part of a CI/CD process, making the automation process easier.
In this architecture, Azure Event Hubs, Log Analytics, and Azure Cosmos DB are identified as a single workload. These resources are included in a single ARM template.
•	Consider staging your workloads. Deploy to various stages and run validation checks at each stage before moving to the next stage. That way you can push updates to your production environments in a highly controlled way and minimize unanticipated deployment issues.
In this architecture there are multiple deployment stages. Consider creating an Azure DevOps Pipeline and adding those stages. Here are some examples of stages that you can automate:
o	Start a Databricks Cluster
o	Configure Databricks CLI
o	Install Scala Tools
o	Add the Databricks secrets
Also, consider writing automated integration tests to improve the quality and the reliability of the Databricks code and its life cycle.
•	Consider using Azure Monitor to analyze the performance of your stream processing pipeline. For more information, see Monitoring Azure Databricks.
For more information, see the DevOps section in Microsoft Azure Well-Architected Framework.
10.	Cost optimization
Cost optimization is about looking at ways to reduce unnecessary expenses and improve operational efficiencies. For more information, see Overview of the cost optimization pillar.
Use the Azure pricing calculator to estimate costs. Here are some considerations for services used in this reference architecture.
11. Azure Databricks
Azure Databricks offers two tiers Standard and Premium each supports three workloads. This reference architecture deploys Azure Databricks workspace in the Premium tier.
Data Engineering and Data Engineering Light workloads are for data engineers to build and execute jobs. The Data Analytics workload is intended for data scientists to explore, visualize, manipulate, and share data and insights interactively.
Azure Databricks offers many pricing models.
•	Pay-as-you-go plan
You are billed for virtual machines (VMs) provisioned in clusters and Databricks Units (DBUs) based on the VM instance selected. A DBU is a unit of processing capability, billed on a per-second usage. The DBU consumption depends on the size and type of instance running Azure Databricks. Pricing will depend on the selected workload and tier.
•	Pre-purchase plan
You commit to Azure Databricks Units (DBU) as Databricks Commit Units (DBCU) for either one or three years. When compared to the pay-as-you-go model, you can save up to 37%.
For more information, see Azure Databricks Pricing.
12.  Azure Cosmos DB
In this architecture, a series of records is written to Azure Cosmos DB by the Azure Databricks job. You are charged for the capacity that you reserve, expressed in Request Units per second (RU/s), used to perform insert operations. The unit for billing is 100 RU/sec per hour. For example, the cost of writing 100-KB items is 50 RU/s.
For write operations, provision enough capacity to support the number of writes needed per second. You can increase the provisioned throughput by using the portal or Azure CLI before performing write operations and then reduce the throughput after those operations are complete. Your throughput for the write period is the minimum throughput needed for the given data plus the throughput required for the insert operation assuming no other workload is running.
13. Example cost analysis
Suppose you configure a throughput value of 1,000 RU/sec on a container. It's deployed for 24 hours for 30 days, a total of 720 hours.
The container is billed at 10 units of 100 RU/sec per hour for each hour. 10 units at $0.008 (per 100 RU/sec per hour) are charged $0.08 per hour.
For 720 hours or 7,200 units (of 100 RUs), you are billed $57.60 for the month.
Storage is also billed, for each GB used for your stored data and index. For more information, see Azure Cosmos DB pricing model.
Use the Azure Cosmos DB capacity calculator to get a quick estimate of the workload cost.
For more information, see the cost section in Microsoft Azure Well-Architected Framework.
