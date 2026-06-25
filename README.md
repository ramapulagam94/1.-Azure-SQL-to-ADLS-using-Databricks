# Azure-SQL-to-ADLS-using-Databricks
This project demonstrates an end-to-end batch data ingestion pipeline built using Azure Databricks, Azure SQL Database, Azure Key Vault, and ADLS Gen2.

**1. Project Overview**



This project implements a batch data ingestion pipeline that extracts customer data from Azure SQL Database, performs column-level filtering and basic transformations using Azure Databricks, stores the processed data in Azure Data Lake Storage Gen2 (ADLS Gen2) as Delta files, and exposes the data through external tables for analytical querying.



The solution follows a simple Medallion-style approach (Bronze->Silver->Gold) where source data is ingested and persisted in an external storage location before being queried through Unity Catalog external tables.







**2. Business Scenario**



A retail company stores operational data in Azure SQL Database.



Source Tables: 



Customer

Orders

Products

Payments

Stores



Customer Analytics:



Revenue Reporting

Product Performance Analysis

Regional Sales Reporting

Executive Dashboards



Since Azure SQL Database is optimized for transactions (OLTP), analytics workloads are offloaded to a Lakehouse architecture.







**3. Business Requirement:**

Business teams require customer information stored in Azure SQL Database to be made available for analytics.



The pipeline should:



- Securely connect to Azure SQL Database.

- Retrieve credentials from Azure Key Vault.

- Access secrets through Databricks Secret Scope.

- Extract source data using JDBC.

- Apply business transformations.

- Store transformed data in ADLS Gen2.

- Create external tables on top of Delta files.

- Query latest data for reporting and analytics.



**4. Project Architecture**



<img width="1819" height="865" alt="Image" src="https://github.com/user-attachments/assets/c972e18b-df6b-4b79-93d6-39582f02dc62" />



**Technology Stack:**



<table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">

    <thead>

        <tr>

            <th>Technology</th>

            <th>Usage</th>

        </tr>

    </thead>

    <tbody>

        <tr>

            <td>Azure SQL Database</td>

            <td>Stores source transactional data such as Customers, Orders, Products, and Payments.</td>

        </tr>

        <tr>

            <td>Azure Key Vault</td>

            <td>Securely stores database credentials, access keys, and sensitive connection information.</td>

        </tr>

        <tr>

            <td>Databricks Secret Scope</td>

            <td>Provides secure access to secrets stored in Azure Key Vault within Databricks notebooks.</td>

        </tr>

        <tr>

            <td>JDBC (Java Database Connectivity)</td>

            <td>Establishes a secure connection between Azure Databricks and Azure SQL Database for data extraction.</td>

        </tr>

        <tr>

            <td>Azure Databricks</td>

            <td>Performs data ingestion, transformation, validation, and processing using distributed computing.</td>

        </tr>

        <tr>

            <td>PySpark</td>

            <td>Implements business logic, data cleansing, filtering, joins, aggregations, and transformations.</td>

        </tr>

        <tr>

            <td>Azure Data Lake Storage Gen2 (ADLS Gen2)</td>

            <td>Stores transformed data files in a scalable and cost-effective cloud storage layer.</td>

        </tr>

        <tr>

            <td>Delta Lake</td>

            <td>Stores data in ACID-compliant Delta format, enabling reliable and performant analytics.</td>

        </tr>

        <tr>

            <td>External Tables</td>

            <td>Provides SQL-based access to Delta files stored in ADLS Gen2 without moving the data.</td>

        </tr>

        <tr>

            <td>Databricks SQL / Spark SQL</td>

            <td>Queries external tables to serve reporting, analytics, and downstream data consumption needs.</td>

        </tr>

    </tbody>

</table>







**7. Key Concepts Demonstrated**



- Secure Credential Management

- Data Ingestion using JDBC

- Data Transformation using PySpark

- External Table Creation

- Data Lake Storage Architecture

- Medallion-Style Data Processing (Bronze → Silver)

- Data Validation & Quality Checks 
