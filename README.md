# Airport Runway Exit Analysis (AREA)

AREA is a Runway Exit Analysis tool built on Apache Spark's Scala API that aims to predict an arriving aircraft's runway exit choice. It is able to use thousands of flight records to train a Random Forest Classification Model. The model then makes predictions on a testing dataset.

*Functionality*:
 - **Pre-processing**: Filters out null data and discards irrelevant columns.
 - **Feature engineering**: Extracts relevant features based on given data (Eg: landing speed from array of positions and epoch times).
 - **Model hyper-parameter tuning using grid-search**: accuracy of each subset measured using cross-validation.
 - **Airport Flexibility**: User is able to input an airport to analyze. It is able to be configured for any runway configuration.
 - **Output**: run information (feature importances, airport info, random forest info) is written to 3 files with a run ID number.

## Installation
Required:

1. Latest version of Java SE Development Kit (JDK)
2. Latest version of Scala
3. Latest version of sbt : www.scala-sbt.org
4. Latest version of Spark

Configure your environment for ease of access.

Scala-IDE for Eclipse is recommended (together with the sbteclipse plugin). Developers can just use:

- sbt compile
- sbt eclipse

and then easily import and run the project in Scala-IDE. However, given that hadoop has been configured and the files inside the /data folder have been uploaded to HDFS, the following commands will also work

- start-dfs.sh
- start-yarn.sh
- sbt package
- spark-submit --class "rtejada.projects.AREA.Main" target/scala-2.11/area_2.11-1.0.jar

Note that dataframes might not be formatted correctly using the latter method, and that it will require enough memory to be configured into the spark-defaults.conf file ($SPARK_HOME/conf). The following settings have been tested to work:

spark.driver.memory 5g
spark.executor.memory 12g


## Usage and Help
Currently supports the following airports:
 - Phoenix(PHX)
 - Atlanta(ATL)
 - Baltimore(BWI)
 - Denver(DEN)

The input will accept the abbreviations (not case-sensitive). 
Every run produces 3 files in an output folder, describing the random forest model, the features used, and other run-specific information.

#### About Spark

 - [Overview](http://spark.apache.org/docs/latest/index.html)
 - [Quick Start](http://spark.apache.org/docs/latest/quick-start.html)
 - [Standalone Mode](http://spark.apache.org/docs/latest/spark-standalone.html)

#### About Scala

 - [Scala Exercises](https://www.scala-exercises.org/std_lib/asserts)

#### About Git
* [Git Download](https://git-scm.com/downloads)
* [Git Basic Tutorial](https://try.github.io/)
* [Git Branching Tutorial](http://learngitbranching.js.org/)

## Contributing
Simple branching model, found [here](https://barro.github.io/2016/02/a-succesful-git-branching-model-considered-harmful/#figure-cactus-model)

Find the section that says: "Something More Simple". There are no need for bugfix/feature branches since it is still a small project, we can work on the master branch for the most part. However, if you want to get accustomed to it in your own branches, here are the default instructions:

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D
