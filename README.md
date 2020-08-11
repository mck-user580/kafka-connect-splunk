## Splunk Connect for Kafka

Clone of the original Splunk Connector with some additional improvements



## Changes
1. Override metadata from headers. 
2. "Inherit" configuration supported
3. Minor improvements


### Supported metadata fields 
* index
* sourcetype
* source
* host
* timestamp (epoch time in millis)

### Metadata configuration 
Splunk metadata can be defined  on 3 ways:
1. Globally for the connector 

    ```
    splunk.index=myindex
    splunk.sourcetype=mysourcetype
    splunk.source=mysource    
    ```
2.  From topic 
  
 ```
# will also work for topics.regex=topic.*
topics=topic1,topic2

...
topics.topic1.splunk.index=myindex1
topics.topic1.splunk.sourcetype=mysourcetype1
topics.topic1.splunk.source=mysource1

topics.topic2.splunk.index=myindex2
topics.topic2.splunk.sourcetype=mysourcetype2
topics.topic2.splunk.source=mysource2
 ```
 3. From message headers
 
supported headers:
* splunk.index
* splunk.sourcetype
* splunk.source
* splunk.host
* splunk.time (epoch time in millis)
