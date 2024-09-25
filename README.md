# FinanceConsumerCompliantPredictionSystem

# About the data
The dataset is about the consumer financial Complaints from the Consumer Financial Protection Bureau(CFPB). This dataset contains information about the consumer complaints related to the financial products and services.These complaints are submitted to the CFPB by consumers. The dataset is regularly updated and includes detailed information about each complaint, which can be instrumental in analyzing trends, identifying problem areas, and predicting outcomes such as whether a complaint leads to the discontinuation of disputed options.

# Accesing the data
The dataset can be downloaded directly from this webpage (https://www.consumerfinance.gov/data-research/consumer-complaints/#download-the-data). The dataset is available in the csv format as well as can be fetched through API key.

# Understanding the dataset

| Features                      | Description                                                                                                                                                   | Data type                  |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| Date received                  | The date the complaint was received by the consumer                                                                                                            | Date                       |
| Product                        | The type of product the consumer identified in the complaint                                                                                                   | Categorical (String)        |
| Sub-product                    | The type of sub-product the consumer identified in the complaint. It's a more specific category within the main product. (Not all products have sub-products)    | Categorical (String)        |
| Issue                          | Specific problem the consumer faced                                                                                                                            | String                     |
| Sub-issue                      | A more specific categorization of the Issue, providing additional detail on the problem                                                                         | String                     |
| Consumer Complaint narrative   | The description of the complaint as submitted by the consumer. This is included only if the consumer consents to publishing it.                                 | String                     |
| Company public response        | This field is relevant for understanding how companies publicly address complaints and whether that has an impact on dispute resolution.                        | Categorical (String)        |
| Company                        | The name of the company the complaint is filed against.                                                                                                        | Categorical (String)        |
| State                          | The state in the U.S. where the complaint was submitted.                                                                                                       | Categorical (String)        |
| ZIP code                       | The ZIP code of the consumer who filed the complaint                                                                                                           | Numerical                  |
| Tags                           | Identifies special consumer groups, such as "Older American", "Servicemember", or "None".                                                                      | Categorical (String)        |
| Consumer consent provided      | Indicates whether the consumer has provided consent for their narrative to be publicly displayed. This determines the availability of consumer narratives for analysis. | Categorical (String)        |
| Submitted Via                  | The medium through which the consumer submitted the complaint                                                                                                  | Categorical (String)        |
| Date sent to the company       | The date the complaint was forwarded by CFPB for response. This is used for timeline analysis to measure how quickly complaints are addressed.                   | Date                       |
| Company response to consumer   | Describes how the company responded to the complaint.                                                                                                          | Categorical (String)        |
| Timely response                | Indicates whether the company responded to the complaint in a timely manner.                                                                                    | Categorical (String)        |
| Consumer disputed              | Indicates whether the consumer disputed the company's response. This is a key target variable for our classification model. It tells us whether the consumer was satisfied or dissatisfied with the company’s resolution. | Binary Categorical (String) |
| Complaint Id                   | It's the unique identifier for the complaint, used for tracking complaints and linking them across the records.                                                 | Int                        |


# Creating the conda environment

```
conda create -n finance python=3.12
```

# Activate Conda environment

```
conda activate finance
```

# Install necessary libraries

```
pip install -r requirements.txt
```




