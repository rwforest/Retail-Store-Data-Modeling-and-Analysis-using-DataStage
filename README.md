# Retail Store Data Modeling and Analysis using DataStage

## Project Overview:

The project implements a star-schema data warehousing flow, then utilize IBM InfoSphere DataStage to develop efficient ETL (Extract, Transform, Load) pipelines to create data marts and perform some analysis on them.

The objective is to store and analyze data from a fictional retail store in a star-schema-based analysis data warehouse while ensuring all changes in the dimensions are preserved.


## Problem Statment:

A fictitious national department store decided to build a star-schema-based sales analysis data warehouse with the dimensions of customer, store, and product. Over time, dimension attributes are expected to change.

The requirement is to preserve versions of all changes in the star-schema data warehouse in order to deliver accurate results with queries that relate to prior versions of dimension attributes.


## Data Source:

The data source for populating the star schema is the store's online transaction processing (OLTP) system.

![Screenshot 2023-05-30 222219](https://github.com/Dina-Hosny/Retail-Store-Data-Modeling-and-Analysis-using-DataStage/assets/46838441/5c80f680-28f9-4289-b900-9c48d26e2256)



## Project Steps:

The IBM InfoSphere DataStage tool is mainly used as an ETL tool to solve the problem.

**1- Building the Physical Data Model:** A star schema is built with three dimensions: ```product```, ```retail```, and ```customer```. And a fact table: ```transactions```.

![ERD with colored entities (UML notation) - ERD with colored entities (UML notation)](https://github.com/Dina-Hosny/Retail-Store-Data-Modeling-and-Analysis-using-DataStage/assets/46838441/57432b46-5ba8-4bb4-a2f5-7daa632c2585)


**2- Extraction:** Extract the data from the data warehouse, ensuring the inclusion of relevant customer, store, and product information.

**3- Transformation:** Perform some transformations on the extracted data to align it with the business needs. This includes converting customer name columns to uppercase.

**4- Data Mart Creation:** A data mart named ```RETAIL_DATA_MART``` is developed. It contains each transaction for each customer, categorized by customer type ("citizen" or "foreign"), information about the product, and the stock purchased. 

**5- Loading:** The transformed data will be loaded into the data mart ```RETAIL_DATA_MART```.

**6- Data Mart Analysis:** The data mart ```RETAIL_DATA_MART``` is used to answer some business needs:

- Display the count of all transactions for each employee in each store.
- Identify the customer type ("citizen" or "foreign") that generates the highest profit. For Example: if "foreign" customers make 5 transactions and citizen "customers" make 3 transactions, the maximum profit would be attributed to foreign customers.
- A bonus is given to the customer(s) who contribute the most to the profit.

## Project Files:

- DataStage Jobs: contains the dsx files for the DataStage jobs
- Dataset: contains the data after it is modeled in a star schema data warehouse model.
- MetaData: contains the metadata of the table to be used in the DataStage jobs.
- Output_Data: contains samples of the data after analyzing it.

## DataStage Jobs:

![Q1](https://github.com/Dina-Hosny/Retail-Store-Data-Modeling-and-Analysis-using-DataStage/assets/46838441/76238f49-8620-4a41-b135-33e7a387ca50)

![Q2 P1](https://github.com/Dina-Hosny/Retail-Store-Data-Modeling-and-Analysis-using-DataStage/assets/46838441/e7b85fde-9b99-450b-be7b-1eb8eeb954c2)

![Q2 P2](https://github.com/Dina-Hosny/Retail-Store-Data-Modeling-and-Analysis-using-DataStage/assets/46838441/87592500-9675-461c-9018-2ce765deb497)

![Q2 P3](https://github.com/Dina-Hosny/Retail-Store-Data-Modeling-and-Analysis-using-DataStage/assets/46838441/9690d9df-58a0-4bcd-b887-91e9b2c6eae8)



## Extended Pipeline (300+ steps)

A production-style version of this project lives in `DataStage Jobs/Pipeline/`: **23 parallel jobs (266 stages) + a master sequence job (36 activities)**. It reuses the same star schema and the three original business questions, but at scale and with the things a real warehouse needs: staging, de-duplication, reject/anomaly capture, **SCD Type 2 customer history** (so queries relate to prior versions of dimension attributes, as the problem statement requires), point-in-time fact loading, marts, data-quality checks and source-to-fact reconciliation.

```
Source_Data/ -> 1_Land -> 2_Dimensions -> 3_Facts -> 4_Marts -> 5_DataQuality
```

- Full inventory, stage mix and the sequence DAG diagram: [`PIPELINE.md`](PIPELINE.md)
- Data: real UCI *Online Retail* order lines (the source of this repo's invoice data) plus synthetic enrichment (customers, stores, employees, suppliers, promotions, FX, calendar). Build it with `python3 pipeline/prepare_sources.py --xlsx "Online Retail.xlsx" [--max-invoices 0]` (the default is a 1,500-invoice sample spread over the year; `0` = all ~541k rows). Download: https://archive.ics.uci.edu/dataset/352/online+retail
- The `.dsx` files are generated: edit `pipeline/pipeline_def.py` and run `python3 pipeline/generate.py`. Column references are validated at build time.
- To import: copy `Source_Data/` to `C:/case_study/Source_Data`, then import `Retail_DW_Pipeline_ALL.dsx` (every job + the sequence) and compile. Directories and dates are job parameters (`pSrcDir`, `pStgDir`, `pDwhDir`, `pMartDir`, `pAuditDir`, `pRunDate`, `pEffDate`).
- `pipeline/run_pipeline.sh` runs the same DAG with `dsjob` (parallel waves) if you prefer to orchestrate outside Designer.

**Status / caveats - please read.** These files were generated against the format of the four exported jobs in this repo, but have **not been imported into a DataStage server** (none was available). The `OrchestrateCode` and transformer code are omitted; Designer regenerates them on compile. Stage types that don't occur in the original jobs (Copy, Funnel, Aggregator in *calculation* mode) and the whole sequence-job export use property encodings I could not verify, so expect to fix a few stage properties on first compile. If the sequence import fails, export any small sequence job from your server and I can match its format. Head stages (top-N) must run in sequential mode to give a global top-N on more than one node.
