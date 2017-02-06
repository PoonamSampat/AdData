# Publishing AdData on GCP 

This project reads the Ad csv from Google Cloud Storage, transforms that into AdInfo object using Google Data Flow. 
Applies a group by date transform to group all records in the csv by date and then inserts a table for each grouped date into Big Query.

# To run the program use the following steps:

1. Upload the csv into Google Cloud Storage.

## Building and Running

The examples in this repository can be built and executed from the root directory by running:
    
    mvn clean package 
    
    mvn clean package && java -cp target/google-cloud-dataflow-java-examples-all-bundled-0.0.1-EXAMPLES.jar 
    com.google.cloud.dataflow.examples.complete.game.AdScoreCheck 
    --project=<PROJECT ID> 
    --stagingLocation= <STAGINGLOCATION>
    --runner=BlockingDataflowPipelineRunner 
    --dataset=<DATASET>
    --input=<INPUTSTORAGELOCATION>

   
This will use the latest release of the Cloud Dataflow SDK for Java pulled from the
[Maven Central Repository](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.google.cloud.dataflow%22).

Make sure to use your project id, not the project number or the descriptive name.
The Cloud Storage location should be entered in the form of
`gs://bucket/path/to/staging/directory`. 

## Querying Tables in Big Query
2.Once run this creates tables in Big Query.

##Run these queries in the BQ console to get the results.

Per Day, per ad impression count.

    SELECT SUM(CLICK_COUNT) AS Total_Impression ,ad_id FROM [project ID.dataset.20170101] where event_type = 'Impression' group by ad_id

Per Day, per ad click count.

    SELECT SUM(CLICK_COUNT) AS Total_Click ,ad_id FROM [project ID.dataset.20170101] where event_type = 'Click' group by ad_id

Ad with highest hit per day 

    SELECT SUM(click_count) as Impression_count, ad_id FROM [project ID.dataset.20170102] where event_type ='Impression' group by ad_id ORDER BY Impression_count DESC LIMIT 1

Ad with least hit per day

    SELECT SUM(click_count) as Impression_count, ad_id FROM [project ID.dataset.20170102] where event_type ='Impression' group by ad_id ORDER BY Impression_count ASC LIMIT 1

Click count across tables. 

    SELECT SUM(click_count) as Impression_count, ad_id FROM [project ID.dataset.20170101],[project ID.dataset.20170102] where event_type ='Impression' group by ad_id ORDER BY Impression_count ASC

#Future Enhancements
1. Templatise dataflow.
