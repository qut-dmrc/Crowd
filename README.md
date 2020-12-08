# Crowd

This Command Line Interface(CLI) tool accesses the CrowdTangle REST API, accesses posts, the leaderboard and the link-checker, stores the data in CSV file and pushes the files into a Google BigQuery database.

# Local installation

You must have access to the CrowdTangle dashboard, read access to Google BigQuery Project and the credential file for the tool to write to BigQuery.

## Environment requirements
- **HomeBrew** Package Management Command Line Interface to install, update and manage software on MacOS
- **Git** Version control system to collaborate as well as to clone the public repos or repos with at least reading access. 
- **Python** Python 3.8.5
- **Anaconda** To create an environment in which all the dependencies/libraries for this CLI tool are installed.

### Using Crowd for the first time
- Create conda environment, remember the name of your environment to activate it everytime you like to use this tool.
```
conda init
conda create --name <replace_this> python=3.8
conda activate <replace_this>   
```
- Clone the code from git repository
```
git clone https://github.com/qut-dmrc/Crowd.git
cd Crowd
pip install -r requirements.txt
```
- Edit the config file(.yml) for your query, different example yaml files are provided for different endpoints
```
Get the API access token from the dashboard setting and paste it in token
Set the history to True if you like to include the counts of emoji interactions over different timestep
Set togbq to True if you like to push your data to Bigquery
Store the credential file in the Crowd folder, and set the bq_credential to <your_credential_file>.json
Set your search terms or links
```
- Build the CLI
`pip install -e .`
- Run the CLI tool to gather your data using the commands below

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