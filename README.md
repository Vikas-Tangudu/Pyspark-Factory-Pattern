# Pyspark-Factory-Pattern
In this project, I leveraged DataBricks to craft multiple ETL pipelines using the Python API of Apache Spark, PySpark, implementing the Factory Pattern for robust and scalable code.

The dataset I utilized comprises sample ecommerce information, encompassing customer details, product specifics, and transaction records, sourced from the source_csv_files folder.

The Factory Pattern emerges as a cornerstone in Data Engineering pipelines, particularly those dealing with diverse data sources. By adhering closely to this pattern, I architected classes for reading, extracting, transforming, loading, orchestrating workflows, and managing sinks.

The Reader class serves as the entry point, accommodating various source formats such as CSV, Parquet, and Delta Tables. Utilizing the Extract class, I employed Spark sessions to ingest the data into a DataFrame, thereby establishing a foundation for subsequent processing.

Within the Transformer class, I employed the PySpark DataFrame API and Spark SQL to enact intricate business logic, transforming the raw data into a refined format conducive to downstream analyses and applications.

The Loader component concludes the pipeline, seamlessly dispatching the transformed data to designated sinks. Here, I implemented two distinct sink strategies: one leveraging Delta Tables for streamlined data management, and another utilizing DBFS files in Parquet format, facilitating compatibility and efficiency.

By adhering to this structured approach and harnessing the capabilities of PySpark within a Factory Pattern paradigm, I ensured the development of production-ready ETL pipelines capable of handling diverse datasets and evolving business requirements.