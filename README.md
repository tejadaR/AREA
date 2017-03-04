# Airport Runway Exit Analysis (AREA)

*AREA is a Runway Exit Analysis tool built on Apache Spark's Scala API that aims to predict an arriving aircraft's runway exit choice. It is able to use thousands of flight records to train a Random Forest Classification Model. The model then makes predictions on a testing dataset.*

# Functionality

 - **Model Training**: Create models by selecting the # of trees and the maximum tree depth.
![Run](docs/images/area_run.png)
- **Airport Flexibility**: Train and test models using data from the currently supported international airports:
   - Phoenix Sky Harbor(PHX)
   - Hartsfield–Jackson Atlanta(ATL)
   - Denver(DEN)
   - Baltimore–Washington(BWI)
 - **Single Landing Testing**: Use trained models to test an exit prediction, by providing paramters
![Load](docs/images/area_load.png)
 - **Results Viewer**: View model details, feature importances, and the structure of each tree in the random foreset.
![View](docs/images/area_view.png)

## Developers

AREA uses a derivation of the model–view–controller (MVC) architectural pattern called **Model-View-Presenter(Supervising Controller)**. The implementation is based on the theory explained here: https://martinfowler.com/eaaDev/SupervisingPresenter.html 

*Behind the scenes functionality:*
 - **Pre-processing**: Filters out null data and discards irrelevant columns.
 - **Feature engineering**: Extracts relevant features based on given data (Eg: landing speed from array of positions and epoch times).
 - **Optional hyper-parameter tuning using grid-search**: accuracy of each subset measured using cross-validation.
 - **Output**: run information (feature importances, airport info, random forest info) is written to 3 files with a run ID number.

AREA handles pre-processing, testing and training tasks asynchronously by using Scala [Futures](http://docs.scala-lang.org/overviews/core/futures.html), **allowing users to concurrently train, load and view different models.**


*Deployment Instructions*:
Scala, JDK, and sbt are required. To deploy the GUI with all dependencies included, use:

> sbt assembly

in the project's root folder. Then run with:

> java -jar /path/AREA-assembly-2.X.jar

If running in Windows, download hadoop's winutils.exe and save as: C:\winutil\bin\winutils.exe

*Developer Instructions*:
Required:

1. Latest version of Java SE Development Kit (JDK)
2. Latest version of Scala
3. Latest version of sbt : www.scala-sbt.org
4. Latest version of Spark

Configure your environment for ease of access.

Scala-IDE for Eclipse is recommended (together with the sbteclipse plugin). Developers can just use:

> sbt compile

> sbt eclipse

and then simply import and run the project in Scala-IDE.

## Other Information

#### About Spark

 - [Overview](http://spark.apache.org/docs/latest/index.html)
 - [Quick Start](http://spark.apache.org/docs/latest/quick-start.html)
 - [Standalone Mode](http://spark.apache.org/docs/latest/spark-standalone.html)

#### About Scala

 - [Scala?] (https://www.scala-lang.org/)
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

