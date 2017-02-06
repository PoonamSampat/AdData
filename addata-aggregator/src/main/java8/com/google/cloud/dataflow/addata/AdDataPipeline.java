package com.google.cloud.dataflow.addata;

import com.google.api.client.util.Lists;
import com.google.api.services.bigquery.Bigquery.Datasets.List;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetField;
import com.google.cloud.bigquery.BigQuery.DatasetOption;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Type;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;


import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.protobuf.WireFormat.FieldType;

import org.apache.avro.reflect.Nullable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This class performs batch processing; reading input from Google Cloud Storage and writing output to
 * BigQuery; using standalone DoFns; use of the group by key transform.
 *
 * <p>Ad Data is ingested and a group by date transform is applied.Then based on this data is written
 * written into Big Query as date tables. The table names are created based on the date gropuped collection's
 * key and the value is the AdInfo Class which holds the Ad Schema.
 * 
 * <p>To execute this pipeline using the Dataflow service and static example input data, specify
 * the pipeline configuration like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 *   --dataset=YOUR-DATASET
 * }
 * </pre>
 * where the BigQuery dataset you specify must already exist.
 *
   */
public class AdDataPipeline implements Serializable {

  /**
   * Class to hold info about an Ad.
   */
  @DefaultCoder(AvroCoder.class)
  static class AdInfo implements Serializable {
    @Nullable String eventType;
    @Nullable String adId;
    @Nullable Integer proposalId;
    @Nullable Integer clickCount;
    @Nullable String time;
    @Nullable String ip;
    @Nullable String geoCode;
    @Nullable Integer osId;
    
    public AdInfo() {}
    
	public AdInfo(String eventType, String adId, Integer proposalId, Integer clickCount, String time, String ip,
			String geoCode, Integer osId) {
		super();
		this.eventType = eventType;
		this.adId = adId;
		this.proposalId = proposalId;
		this.clickCount = clickCount;
		this.time = time;
		this.ip = ip;
		this.geoCode = geoCode;
		this.osId = osId;
	}
	
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public String getAdId() {
		return adId;
	}
	public void setAdId(String adId) {
		this.adId = adId;
	}
	public Integer getProposalId() {
		return proposalId;
	}
	public void setProposalId(Integer proposalId) {
		this.proposalId = proposalId;
	}
	public Integer getClickCount() {
		return clickCount;
	}
	public void setClickCount(Integer clickCount) {
		this.clickCount = clickCount;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getGeoCode() {
		return geoCode;
	}
	public void setGeoCode(String geoCode) {
		this.geoCode = geoCode;
	}
	public Integer getOsId() {
		return osId;
	}
	public void setOsId(Integer osId) {
		this.osId = osId;
	}
	
	public String getKey(String keyname) {
      if (keyname.equals("adId")) {
        return this.adId;
      } else { //return ip as default
        return this.time;
      }
    }
  }

  /**
   * Parses the raw ad info into AdInfo objects. Each event line has the following
   * event_type,ad_id,proposal_id,click_count,date_time,ip,geo_code,os_id
   * Click,3232,101,4,1/3/2017,10.1.1.1,US-WA,1
   */
  static class ParseDataFn extends DoFn<String, AdInfo> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseDataFn.class);
    
    private final Aggregator<Long, Long> numParseErrors =
        createAggregator("ParseErrors", new Sum.SumLongFn());    
    
    @Override
    public void processElement(ProcessContext c) {
      String[] components = c.element().split(",");
      try {
    	  
    	String eventType = components[0].trim();
    	String adId = components[1].trim();
    	Integer proposalId = Integer.parseInt(components[2].trim());
		Integer clickCount = Integer.parseInt(components[3].trim());
		String time = components[4].trim();
		String ip = components[5].trim();
		String geoCode = components[6].trim();
		Integer osId = Integer.parseInt(components[7].trim());
    	  
        AdInfo adInfo = new AdInfo(eventType, adId, proposalId, clickCount, time, ip, geoCode, osId);
        c.output(adInfo);
        
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        numParseErrors.addValue(1L);
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }
  
  public static class WriteData extends DoFn<KV<String, Iterable<AdInfo>>, AdInfo> implements Serializable {

	  private static final Logger LOG = LoggerFactory.getLogger(WriteData.class);
	  
	  private final Aggregator<Long, Long> numParseErrors = createAggregator("ParseErrors", new Sum.SumLongFn());

	  @Override
	  public void processElement(ProcessContext c) {

	  KV<String, Iterable<AdInfo>> adItems = (KV) c.element();
  	  try {

  		  	BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

   		  	Dataset dataset = null;
	           	
  		  	Options options=(Options)PipelineOptionsFactory.as(Options.class);
	      
  		  	dataset = bigquery.getDataset(options.getDataset());
	                	     
  		  	Schema schema = Schema.of(Field.of("event_type", Type.string()), Field.of("ad_id", Type.string()),
  		  			Field.of("time", Type.string()), Field.of("click_count", Type.integer()), Field.of("ip", Type.string()));
	     
  		  	StandardTableDefinition definition = StandardTableDefinition.newBuilder().setSchema(schema).build();
	        
  		  	//Generating the table name based on the Grouped Date Column.
  		  	Table table = dataset.get(adItems.getKey().replaceAll("_", ""));
	      	      
  		  	if(table == null) {
		     	table = dataset.create(adItems.getKey().replaceAll("_", ""), definition);
  		  	} 
	          	     
  		  	ArrayList<RowToInsert> rows = new ArrayList<RowToInsert>();
	     
  		  	for (AdInfo adInfo : adItems.getValue()) {
	    	   	
	    	    Map<String, Object> rowContent = new HashMap<String, Object>();
	    	   	 
	    	    rowContent.put("ad_id", adInfo.getAdId());
	    	    rowContent.put("event_type", adInfo.getEventType());
	    	    rowContent.put("time", adInfo.getTime());
	    	    rowContent.put("click_count", adInfo.getClickCount());
	    	    rowContent.put("ip", adInfo.getIp());
	    	    
	    	    //Inserting records from the grouped date collection.
	    	    InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(table.getTableId()).addRow(rowContent).build());
	    	        	   
	      	    }
  	  		} 
		  catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
			  numParseErrors.addValue(1L);
			  LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
		  }
	  	}
	  }
	    
  /**
   * A transform to group AdData by Date AdInfo.
   */
  // [START DocInclude_USExtractXform]
	public static class GroupByDate
			extends PTransform<PCollection<AdInfo>, PCollection<KV<String, Iterable<AdInfo>>>> {

		private final String field;

		GroupByDate(String field) {
			this.field = field;
		}
		//Groups the Collection based on the Date which is a part of the records,
		//creates a <Date,List<Rest of the Records>>
		public PCollection<KV<String, Iterable<AdInfo>>> apply(PCollection<AdInfo> adInfo) {

			return adInfo.apply(MapElements.via((AdInfo aInfo) -> KV.of(aInfo.getKey(field), aInfo))
					.withOutputType(new TypeDescriptor<KV<String, AdInfo>>() {
					})).apply(GroupByKey.<String, AdInfo>create());
		}
	}
  // [END DocInclude_USExtractXform]


  /**
   * Options supported by {@link UserScore}.
   */
  public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing game data.")
    // The default maps to two large Google Cloud Storage files (each ~12GB) holding two subsequent
    // day's worth (roughly) of data.
    @Default.String("gs://addatademo/InputSample.csv")
    String getInput();
    void setInput(String value);

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    @Default.String("dataflow_examples")
    String getDataset();
    void setDataset(String value);
    
    @Description("DataFlow Project. Must already exist.")
    @Validation.Required
    @Default.String("dataworkflow-156112")
    String getProject();
    void setProject(String value);

    @Description("The BigQuery table name. Should not already exist.")
    @Default.String("ad_score")
    String getTableName();
    void setTableName(String value);
  }

  
  /**
   * Run a batch pipeline.
   */
 // [START DocInclude_USMain]
  public static void main(String[] args) throws Exception {
	    
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

	// Read events from a text file and parse them.
	pipeline.apply(TextIO.Read.from(options.getInput()))
	
	//Convert to AdInfo Object
	.apply(ParDo.named("ParseAdInfoData").of(new ParseDataFn()))
	
	// Group the Ad data by Date in the incoming file.
	.apply("GroupAdDatabyDate", new GroupByDate("date_time"))
	
	//Write Data in to BigQuery for each date key
	.apply(ParDo.named("WriteData").of(new WriteData()));
	  
    // Run the batch pipeline.
    pipeline.run();
  }
  // [END DocInclude_USMain]

}
