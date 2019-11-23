# Nifi custom processor
## Introduction
This project is to build a custom processor to perform the task of hashing specific columns with interested algorithms including: MD2, MD5, SHA224, SHA256 and SHA512. As our particular purpose that required the outcome is in csv format, then the csv output support is included in this project as well.

There are standard hashing processors but they mostly work with flowfile's attribute or a whole content, while we only interest in partial content data. You can directly download the compiled output file [HERE](./misc/nifi-HashColumn-nar-1.0.nar) and test with your data flow. (put nar file in `lib` folder of Nifi installed location, restart required to get the imported processor showing up)

<p align="center">
    <image src="./misc/add_processor.png" width="40%"/>
    <image src="./misc/config_properties.png" width="40%"/>
</p>

## Dependencies
This is a list of additional libraries that not come along during generating nifi-processor Maven's archetype template.
* `avro`: from apache avro lirary to make the work with generic record easier
* `commons-csv`: from apache-common to work with csv format

## Get the project run
Ensure your computer is installed the following:
* Java 8 JDK
* Maven

    git clone https://github.com/vanducng/hashing-columns-nifi-processor.git
    cd hashing-columns-nifi-processor
    mvn clean install

The output is located at `.\hashing-columns-nifi-processor\nifi-HashColumn-nar\target\nifi-HashColumn-nar-1.0.nar`

## Future improvement
* CSV format can be configured with numerous configuration properties such as value separator, record separator, quote character, escape charactor, etc.
* Add in JSON output format support since adding another conversion processor will impact the processing time of the whole data flow.

## Resources
- [YOUTUBE LINK](https://www.youtube.com/watch?v=v2u0WsPs2Ac): Custom Processor Development with Apache NiFi: a very informative resource to get the sense of custom processor development.

- [APACHE.ORG](https://nifi.apache.org/developer-guide.html): Official development document reference. 

- [MEDIUM LINK](https://medium.com/@g22shubham/apache-nifi-part-i-create-custom-processor-675fcf251a1): Build a first simple custom processor.
    
- [APACHE.ORG](https://avro.apache.org/docs/current/gettingstartedjava.html): Working with Apache Avro
    
- [BLOG LINK](https://www.callicoder.com/java-read-write-csv-file-apache-commons-csv/): Working with Apache Common CSV