# CollegeScoreCardAnalysis

In this Project, I analyzed the data and answered the following questions;

- ```How much has the price gone up on the cost of college over the last few years?```
- ```Which States cost the most when it comes to attending college?```
- ```Which States has the highest number of student?```
- ```get cost per year on private instituition```

## Project Details

In this Project, I have set up and used the following tools

- Docker : to run the entire pipeline, package the ETL and build a container
- Jupyter Notebook : For testing and experiment functions before packaging into a script
- Bash Scripting : To aggregate and move files from different directory into a data directory for ease access and reusability
- Pytest : to run code tests

### Data Source

Data was sourced from [here](https://collegescorecard.ed.gov/data/)

- Institution-level data files for 1996-97 through 2019-20 containing aggregate data for each institution.Includes information on institutional characteristics, enrollment, student aid, costs, and student outcomes.
- Field of study-level data files for the pooled 2014-15, 2015-16 award years through the pooled 2016-17, 2017-18 award years containing data at the credential level and 4-digit CIP code combination for each institution. Includes information on cumulative debt at graduation and earnings one year after graduation.
- Crosswalk files for 2000-01 through 2018-19 that link the Department’s OPEID with an IPEDS UNITID for each institution.

Inspiration : [confession of a data guy](https://www.confessionsofadataguy.com/build-your-data-engineering-skills-with-open-source-data/)
