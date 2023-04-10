# ETL-Airflow
This project is an example of an ETL (Extract, Transform, Load) pipeline implemented using Apache Airflow. The goal of the pipeline is to extract data from a PostgreSQL database, transform it, and load it into another PostgreSQL database. The data in the source database represents user events on a website, and the data in the destination database represents a denormalized version of the user events, organized by user.

The etl_task.ipynb notebook contains the code for the ETL pipeline. It is structured as follows:

- Step 1: Import Libraries and Set Up the DAG - Imports the necessary libraries and sets up the DAG (Directed Acyclic Graph) for the pipeline.

- Step 2: Define the Operators - Defines the custom operators used in the DAG, including operators for extracting data from the source database, transforming the data, and loading the data into the destination database.

- Step 3: Define the Tasks - Defines the tasks for the DAG, which include the custom operators defined in step 2 as well as some built-in Airflow operators for scheduling and monitoring the pipeline.

- Step 4: Configure the DAG - Configures the DAG by setting the task dependencies and the default arguments for the tasks.

- Step 5: Test the Pipeline - Tests the pipeline by running it manually in the notebook.

To use this pipeline, you will need to have Apache Airflow installed on your system, as well as PostgreSQL databases for the source and destination data. You will also need to update the code to use the correct database connection information.
![image](https://user-images.githubusercontent.com/74065724/230910604-390cc3e3-b406-448c-af3c-335d5774155a.png)

Here is a brief description of each task:

- extract_feed(): This task extracts data from the feed_actions table in the simulator database. It selects various columns such as event_date, user_id, gender, age, os, views, and likes. It groups the data by event_date, user_id, gender, age, and os. It returns a Pandas DataFrame.

- extract_message(): This task extracts data from the message_actions table in the simulator database. It selects various columns such as event_date, user_id, gender, age, os, messages_received, messages_sent, users_received, and users_sent. It groups the data by event_date, user_id, gender, age, and os. It returns a Pandas DataFrame.

- merge(df_feed, df_message): This task merges the DataFrames extracted by extract_feed() and extract_message() into a single DataFrame. It returns the merged DataFrame.

- to_clickhouse(): This task convey to ClickHouse the data which was brought by previous tasks
- gender(df): This task groups the merged DataFrame by event_date and gender. It calculates the sum of the likes, views, messages_received, messages_sent, users_received, and users_sent columns for each group. It adds a new column dimension with a value of gender, renames the gender column to dimension_value, and returns a DataFrame.

- os(df): This task groups the merged DataFrame by event_date and os. It calculates the sum of the likes, views, messages_received, messages_sent, users_received, and users_sent columns for each group. It adds a new column dimension with a value of os, renames the os column to dimension_value, and returns a DataFrame.

- age(df): This task groups the merged DataFrame by event_date and age. It calculates the sum of the likes, views, messages_received, messages_sent, users_received, and users_sent columns for each group. It adds a new column dimension with a value of age and returns a DataFrame.



