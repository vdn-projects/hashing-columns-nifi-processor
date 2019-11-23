# Nifi custom processor
## Introduction
This project is to build a custom processor to perform the task of hashing specific columns with interested algorithms including: MD2, MD5, SHA224, SHA256 and SHA512. Since our project need the outcome is in csv format, then the .

## Dependencies
This is a list of additional libraries that not come along during generating nifi-processor Maven's archetype template.
* `commons-csv`: from apache-common to work with csv format

## How to build the custom processor
It is recommended to use InteliJ IDE to build this project, separated Apache Maven can be used to compile and generate custom processor package.

    git clone 
    cd nifi-custom-processor\nifi-hashor-nar
    mvn clean install

## Future improvement
* CSV format can be configured with numerous configuration properties such as value separator, record separator, quote character, escape charactor, etc.
* Add in support for another common output file like JSON.

## Resources
- Custom Processor Development with Apache NiFi: a very informative resource to get the sense of custom processor development.

    [YOUTUBE LINK](https://www.youtube.com/watch?v=v2u0WsPs2Ac)
- Build a first simple custom processor
    
    [MEDIUM LINK](https://medium.com/@g22shubham/apache-nifi-part-i-create-custom-processor-675fcf251a1)
- Working with Apache Avro
    
    [APACHE.ORG](https://avro.apache.org/docs/current/gettingstartedjava.html)
- Working with Apache Common CSV
    
    [BLOG LINK](https://www.callicoder.com/java-read-write-csv-file-apache-commons-csv/)
    
    https://nifi.apache.org/developer-guide.html
