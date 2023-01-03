# ReCiterDB

## Summary

ReCiterDB is an open source [MariaDB](https://mariadb.org/) database and set of tools that stores publication lists and computes bibliometric statistics for an academic institution's faculty and other people of interest. ReCiterDB is designed to be populated by person and publication data from [ReCiter](https://github.com/wcmc-its/reciter) (a machine learning-driven publication suggestion engine) and from third party sources such as NIH's iCite and Digital Science's Altmetric services. The data in the system can be viewed using the [ReCiter Publication Manager](https://github.com/wcmc-its/reciter-publication-manager) web application, or it can serve as a stand alone reporting tool. For more on the functionality in Publication Manager, see that repository.

This repository contains:

- A MariaDB (an SQL fork) schema for ReCiterDB
- Stored procedures and events for populating and updating that database
- Python and shell scripts for importing data into ReCiterDB
- A Docker file that automates deployment of these components

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

All of the above are packaged in a Docker file.

...to provide...


## Components
ReCiterDb consists of the following components:


|File name |Expected frequency |Type |Frequency |
| ---- | ------------- | ------------- | ---------- |
| **setupReciterDB.py**	| At initial setup	| Python script |Runs three below SQL files which create the database, inserts certain data, and events and procedures.|
|createDatabaseTableReciterDb.sql |	At initial setup |	Database schema	 | Creates ReCiterDb database and the following tables: <br> &bull; `admin_*` - tracks users, their roles, and their feedback in Publication Manager <br> &bull; `analysis_altmetric_*` - bibliometric article-level data from Altmetric API <br> &bull; `analysis_override_author_position` - a table for manually overriding the inferred author position; there is no way to update these values through the web user interface <br> &bull; `analysis_nih_*` - bibliometric article-level data from NIH's iCite API <br> &bull; `analysis_summary_*` -  periodically updated, summary-level index tables for articles, authorships, and people; the people included in the analysis_summary_person table reflect the list contained in the `analysis_summary_person_scope` table, which is maintained by the system admin; these tables are widely used <br> &bull; `analysis_special_characters` -  includes special character to RTF lookups used for generating RTF files <br> &bull; `analysis_temp_*` - temporary tables used for staging data so that they can be used for outputting files <br> &bull; `journal_*` - metadata about journals from third-party sources <br> &bull; `person_*` - data imported directly from ReCiter's Feature Generator API|



## Configuration
- **Define scope of bibliometrics.** As an administrator, you have control over the people for whom the system calculates person-level bibliometrics. This allows for download of a person's bibliometric analysis complete with comparisons to institutional peers. To do this, update the populateAnalysisSummaryPersonScopeTable stored procedure which populates the analysis_summary_person_scope table. Here at Weill Cornell Medicine, we consider only full-time employed faculty (i.e., person_person_type.personType = academic-faculty-weillfulltime).
- **Importing additional journal-level metrics (optional).** ReCiterDB ships with journal impact data from Scimago Journal Rank. If you have another journal level impact metric, which uses ISSN as a primary key, it can be imported into the journal_impact_alternative table.


## More on the ReCiter suite of applications

As the figure describes, the ReCiter suite of applications can fully manage many key steps in institutional publication management.

The key tools and repositories used to perform these steps are:

[table here]
