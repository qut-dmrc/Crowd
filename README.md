# Crowd

This Command Line Interface(CLI) tool accesses the CrowdTangle REST API, accesses posts, the leaderboard and the link-checker, stores the data in CSV file and pushes the files into a Google BigQuery database.

# Local installation

You must have access to the CrowdTangle dashboard, and optionally read access to Google BigQuery Project and the credential file for the tool if you wish to write to BigQuery.

## Environment requirements
- **HomeBrew** Package Management Command Line Interface to install, update and manage software on MacOS
- **Git** Version control system to collaborate as well as to clone the public repos or repos with at least reading access. 
- **Python** Python 3.8.5

### Using Crowd for the first time
- Clone the code from git repository
```
git clone https://github.com/qut-dmrc/Crowd.git
cd Crowd
pip3 install virtualenv
virtualenv env
[Windows] "env/Scripts/activate.bat" [MAC] source env/bin/activate
pip install -r requirements.txt
```
- Edit the config file(.yml) for your query, `insta_config_template.yml` provides you the template
  * Get the API access token from the dashboard setting(gear icon)-> API Access-> copy the token and paste it in between double quotes for token 
  * Set the history to True if you like to include the counts of emoji interactions over different timestep
  * Set togbq to True if you like to push your data to Bigquery [Optional]
  * Store the credential file in the Crowd folder, and set the bq_credential to <your_credential_file>.json [Optional]
  * Set your search terms or links and the rest of configuaration accordingly to your query.
- Build the CLI `pip install -e .`
- Run the CLI tool to gather your data using the commands in Usage section.

### Using Crowd Subsequently
- Activate the environment `[Windows] "env/Scripts/activate.bat" [MAC] source env/bin/activate`
- [Optional] Run it every time there is a new change to the code `pip install -e .`
- Edit the yaml file for your query. 
- `crowd -c <your_config>.yml`

### Install New Release of the Code
```
git stash
git pull
git stash pop
```
Follow the steps in # Using Crowd Subsequently # to use the tool. 

## Usage

### Command Line
```
$ Crowd
Usage: crowd [OPTIONS]

Commands:
  crowd [OPTIONS]      searches the entire, cross-platform CrowdTangle system of posts

OPTIONS
  -c/--config          A config file associated with a query                           [string][default:default_query.yml]
  --help

Examples:
  1. crowd                                         Run the query with params set in default_query.yml
  2. crowd -c "myquery.yml"                        Run the query with params set in "myquery.yml"
  3. crowd -c "myquery.yml" -a                     Append to the csv file stated in myquery.yml file


Source code available at https://github.com/qut-dmrc/Crowd.git