# ReCiterDB
- [Summary](#summary)
- [Functionality](#functionality)
- [Technical](#technical)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Locally](#locally)
    - [On AWS](#on-aws)
- [Components](#components)
    - [Setup](#setup)
    - [Update](#update)
  - [Configuration](#configuration)
- [More on the ReCiter suite of applications](#more-on-the-reciter-suite-of-applications)



## Summary

ReCiterDB is an open source [MariaDB](https://mariadb.org/) database and set of tools that stores publication lists and computes bibliometric statistics for an academic institution's faculty and other people of interest. ReCiterDB is designed to be populated by person and publication data from [ReCiter](https://github.com/wcmc-its/reciter) (a machine learning-driven publication suggestion engine) and from third party sources such as NIH's iCite and Digital Science's Altmetric services. The data in the system can be viewed using the [ReCiter Publication Manager](https://github.com/wcmc-its/reciter-publication-manager) web application, or it can serve as a stand alone reporting tool. For more on the functionality in Publication Manager, see that repository.

This repository contains:

- A MariaDB (an SQL fork) schema for ReCiterDB
- Stored procedures and events for populating and updating that database
- Python and shell scripts for importing data into ReCiterDB
- A Docker file that automates deployment of these components

<img src="https://github.com/wcmc-its/ReCiterDB/blob/master/files/reCiterReportingModel.png" width=800 />



## Functionality
In conjunction with data from [ReCiter](https://github.com/wcmc-its/reciter), ReCiterDB has been used to answer questions such as the following:

- Senior-authored academic articles in Department of Anesthesiology
- Percentage of full-time faculty publications that were indexed in PubMed with an ORCID identifier
- Publications by full-time faculty added in the past week
- h5 index of full-time faculty
- Which active full-time faculty does any given faculty cite most often on their papers?
- Which faculty publish the most frequently on cancer, overall and by proportion of their total scholarly output?
- What percent of papers published by a given faculty are in collaboration with existing members of the Cancer Center?
- What are the most influential cancer-related papers by members of the Cancer Center?
- Finally, a variety of person-level bibliometric statistics are available through a bibliometric report that can be generated on demand (see [sample](https://github.com/wcmc-its/ReCiterDB/blob/master/files/sampleBibliometricReport.rtf))

## Technical

### Prerequisites
- **Installed recent version of MariaDB**. It's important to use MariaDB (a fork of MySQL) as opposed to MySQL because the stored procedures that ship with ReCiterDB include several functions that are uniquely supported by MariaDB.
- **Populated instance of ReCiter**. This is where all the person and publication data live.
- **Installation of ReCiter Publication Manager (optional)**. Needed in case you wish to interact with the data (curate, report on, etc.) in ReCiterDB through a web user interface.

### Installation

#### Locally

1. Download [repository](https://github.com/wcmc-its/ReCiterDB/archive/refs/heads/master.zip) to local directory.
2. Unzip and move to desired directory.
3. Ensure both the `reciterDbImport.sh` and `retrieveUpdate.sh` shell scripts are executable. You can do so in Terminal by navigating to the directory where these files are located and running the following commands:
```
chmod +x reciterDbImport.sh
chmod +x retrieveUpdate.sh
```
4a. For each new window in Terminal, you need to assert the following environmental variables. Note that you would need to know the values for you database first before running these commands.
```
export DB_HOST=[db host]
export DB_USERNAME=[user]
export DB_PASSWORD=[password]
export DB_NAME=[db name]
export AWS_ACCESS_KEY_ID=[access key ID]
export AWS_SECRET_ACCESS_KEY=[secret access key]
export AWS_DEFAULT_REGION=[region]
```
4b. If you haven't done so, run `python3 setupReciterDB.py`. This will set up the database and schema. This script should execute in seconds. 
5. To update ReCiterDB on a daily basis, run `python3 retrieveUpdate.sh`. This script may take 45 minutes to execute.



#### On AWS

All of the above are packaged in a Docker file.

...to provide...


## Components
ReCiterDb consists of the following components:


### Setup

|File name |Expected frequency |Type |Purpose |
| ---- | ------------- | ------------- | ---------- |
| **setupReciterDB.py**	| At initial setup	| Python script |Runs three below SQL files which create the database, inserts certain data, and events and procedures.|
|createDatabaseTableReciterDb.sql |	At initial setup |	Database schema	 | Creates ReCiterDb database and the following tables: <br> &bull; `admin_*` - tracks users, their roles, and their feedback in Publication Manager <br> &bull; `analysis_altmetric_*` - bibliometric article-level data from Altmetric API <br> &bull; `analysis_override_author_position` - a table for manually overriding the inferred author position; there is no way to update these values through the web user interface <br> &bull; `analysis_nih_*` - bibliometric article-level data from NIH's iCite API <br> &bull; `analysis_summary_*` -  periodically updated, summary-level index tables for articles, authorships, and people; the people included in the analysis_summary_person table reflect the list contained in the `analysis_summary_person_scope` table, which is maintained by the system admin; these tables are widely used <br> &bull; `analysis_special_characters` -  includes special character to RTF lookups used for generating RTF files <br> &bull; `analysis_temp_*` - temporary tables used for staging data so that they can be used for outputting files <br> &bull; `journal_*` - metadata about journals from third-party sources <br> &bull; `person_*` - data imported directly from ReCiter's Feature Generator API|
| insertBaselineDataReciterDb.sql	| At initial setup	| Data to be imported	| Imports following data into existing tables: &bull; roles for Publication Manager application <br /> &bull; special characters and their RTF equivalents <br /> &bull; Scimago journal rankings <br /> &bull; National Library of Medicine (NLM) journals in PubMed | 
| createEventsProceduresReciterDb.sql	| At initial setup	| Stored procedures & events | 	Creates stored procedures which are used to: <br /> &bull;  populate the `analysis_summary_*`  tables, which function as a performant index, and is useful for querying<br /> &bull; generate RTF files Create events that are used for executing certain stored procedures on a nightly basis.|


### Update

|File name |Expected frequency |Type |Purpose |
| ---- | ------------- | ------------- | ---------- |
| **retrieveUpdate.sh** |	Daily	|Shell script	| Orchestrates the execution of the below five scripts. The expectation would be that this script would run and refresh reporting and bibliometric data on a nightly basis.| 
| retrieveS3.py	| Daily	| Python script	| Retrieves article and person data from the AWS s3 instance where your ReCiter is installed.| 
| retrieveDynamoDb.py | Daily	| Python script	| Retrieves article data from the AWS DynamoDb instance where your ReCiter is installed.| 
| retrieveNIH.py	| Daily	| Python script	| Retrieves list of PMIDs from ReCiterDB and looks up article-level statistics from [NIH's iCite RCR service](https://icite.od.nih.gov/). These statistics are used to generate bibliometrics.|
| retrieveAltmetric.py	| Daily	| Python script	| Retrieves list of PMIDs from ReCiterDB and looks up article-level statistics from Digital Science's Altmetric service. As of Fall 2022, this requires an API key, which in turn requires providing and getting your research use case approved. |
| updateReciterDB.py	| Daily	| Python script	| Takes data generated from retrieveS3.py and retrieveDynamoDb.py scripts and loads them into ReCiterDB |



## Configuration
- **Define scope of bibliometrics.** As an administrator, you have control over the people for whom the system calculates person-level bibliometrics. This allows for download of a person's bibliometric analysis complete with comparisons to institutional peers. To do this, update the populateAnalysisSummaryPersonScopeTable stored procedure which populates the `analysis_summary_person_scope` table. Here at Weill Cornell Medicine, we consider only full-time employed faculty (i.e., `person_person_type.personType = academic-faculty-weillfulltime`). 
- **Importing additional journal-level metrics (optional).** ReCiterDB ships with journal impact data from Scimago Journal Rank. If you have another journal level impact metric, which uses ISSN as a primary key, it can be imported into the journal_impact_alternative table.


## More on the ReCiter suite of applications

As the figure describes, the ReCiter suite of applications can fully manage many key steps in institutional publication management.

<img src="https://github.com/wcmc-its/ReCiterDB/blob/master/files/howReciterWorks.png" width=800 />


The key tools and repositories used to perform these steps are:
|Repository |Required? |Functionalities |
| ---- | ------------- | ------------- |
| [ReCiter](https://github.com/wcmc-its/ReCiter)	|yes|	 &bull; Store identity info (see #1 above)  <br> &bull; Coordinate retrieval of articles from PubMed and optionally Scopus  <br> &bull; Use machine learning to estimate the likelihood a scholar wrote each article (#3)  <br> &bull; Store a person's identity and articles  <br> &bull; Share data through web services (#4, #5)|
| [ReCiter PubMed Retrieval Tool](https://github.com/wcmc-its/ReCiter-Scopus-Retrieval-Tool) | yes	|  &bull; Retrieve and normalize publication data from PubMed (#2) |
| [ReCiter Scopus Retrieval Tool](https://github.com/wcmc-its/ReCiter-PubMed-Retrieval-Tool) | no	|  &bull; Retrieve and normalize publication data from Scopus (#2) |
| [ReCiter Publication Manager](https://github.com/wcmc-its/ReCiter-Publication-Manager) | no	|  &bull; Collect feedback from librarians, department staff on most likely articles a given researcher has authored (#4) &bull; Provides a web interface for generating reports (#6)|
| [ReCiterDB](https://github.com/wcmc-its/ReCiterDB) | optional but would be needed for Publication Manager | &bull; A set of scripts for retrieving data from ReCiter and populating the database (#5)  <br> &bull; A relational database for storing publication and bibliometric data (#6)|
