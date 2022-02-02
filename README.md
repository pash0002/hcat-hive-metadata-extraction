# **hive-metadata-extraction**

#### Steps to Build Jar

1. Move to project directory i.e. hive-metadata-extraction
2. Run JAR building command `./gradlew clean shadowJar`


#### Step to Run JAR File

`java -jar hive-metadata-extraction.jar hive_config.properties`
   
   OR
   
   `java -jar -Dlog4j.configurationFile=./log4j2.xml hive-metadata-extraction.jar hive_config.properties` 


#### hive_config.properties

1. **thriftUrl** = URL to access hive metadata via HCat
2. **kerberos.enabled** = set true when kerberos is enabled else set to false
3. **sasl** = set true when having the kerberos metastore else set to false
4. **kerberos.hive.principal** = kerberos principal of hive to access metadata
5. **javax.security.auth.useSubjectCredsOnly** = This need to set false when having kerberos hive else set it to true
5. **outputDirectory** = Directory where the dump will be created
6. **beautifyJson** = set it to true for formatted json