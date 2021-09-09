### Stream Processing of vital signal with Apache Flink
#### Stream processing with Apache Flink

This repo contains the code developed for the Master Thesis "Distributed Stream Processing and Modeling of Multivariate Temporal Sequences in the Medical Domain". It includes a Scala library for Flink, which contains different functions to perform preprocessing operations and feature construction over time series, on streaming.

Besides, it includes a pre-trained model for performing classification, as well as some datasets to test the functions implemented.

The code developed for Scala is available under src/main/scala directory. Folder util/ contains all the functions implemented: aggregation, feature extraction, interpolation, normalization, etc. Examples of their usage are available under experiments/. Moreover, a ProcessWindowFunction class, LSTMSequenceClassifier.scala was created in order to load a previously trained LSTM classifier and obtain classifications over data from a Flink window. Examples of the usage of this class are available under experiments/LSTMTest.scala and experiments/IntegratedExperiment.scala. This last experiment (experiments/IntegratedExperiment.scala) includes a complete workflow example in which raw time series from bedside monitors are fed to the program, they are processed, and then they are classified with the LSTM model as shock or non-shock sequences.

More information about this project is available in the thesis document.

Requirements: 
- Java 8.0 
- Scala 2.12
- Apache Flink 1.13.0

- [Apache Flink](https://flink.apache.org/)
- [Apache Application Development, DataStream API](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html)
- [A historical account of Apache Flink ](https://www.dima.tu-berlin.de/fileadmin/fg131/Informationsmaterial/Apache_Flink_Origins_for_Public_Release.pdf)



