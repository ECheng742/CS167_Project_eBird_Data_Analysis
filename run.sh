# Currently, the project refuses to run on the command line, even with dependencies.
# It will be compiled and packaged by maven, but when run without dependencies fails to detect the required
# libraries. However, when compiled with dependencies, produces a stranger error still:
# Exception in thread "main" java.lang.RuntimeException:
# Input format 'shapefile' cannot be recognized. Perhaps you mean one of {}
# This error appears to be because Beast does not play well with compiling with dependencies.
# Note that the entire project runs in IntelliJ without any errors or issues, and we were able to all
# complete our parts and run them to generate the maps and graphs properly.
# note that I am compiling with dependencies as detailed in this post: https://www.sohamkamani.com/java/cli-app-with-maven/
mvn clean compile assembly:single
java -jar final_project-1.0-SNAPSHOT-jar-with-dependencies.jar prepare eBird_1k.csv.bz2
java -jar final_project-1.0-SNAPSHOT-jar-with-dependencies.jar spatialAnalysis Mallard
java -jar final_project-1.0-SNAPSHOT-jar-with-dependencies.jar pie-chart 02/21/2015 05/22/2015