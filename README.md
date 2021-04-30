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
- You need to have a config file in `Crowd` folder in order to run script
  * Create a `your_config.yml` file, different example yaml files are provided in `config_template` folder accordingly for different endpoints. Please refer to Config Templates section to select the right template for you
  * Select the template for your case, copy and paste it `your_config.yml` 
  * Get the API access token from the dashboard setting and paste it in token
  * Set the history to True if you like to include the counts of emoji interactions over different timestep
  * Set togbq to True if you like to push your data to Bigquery
  * Store the credential file in the Crowd folder, and set the bq_credential to <your_credential_file>.json [Creating service account to get the credential file](https://cloud.google.com/docs/authentication/getting-started)
  * Set your search terms or links and the rest of configuaration accordingly to your query.
- Build the CLI `pip install -e .`
- Run the CLI tool to gather your data using the commands in Usage section.

#### Config Templates
- **config_links.yml**: `Links` endpoint will retrieve a set of posts matching a certain link.
- **config_search_terms.yml**: This template will search via `posts\search` endpoint, search_terms **MUST** be included to retrieve posts that match the search terms in the entire CT database. Search range can be futhered refined by list(s) of accounts and a time frame. 
- **config_no_search_term.yml**: This template will search via `posts` endpoint, it serves similar purpose to that of `posts/search` endpoint except for that search_term can be left blank to curate all the posts from list(s) of accounts within/without a certain time range.
- **config_post.yml**: This template is to collect data for specific post(s) by their post id(s) for further investigation.

**Parameters**
```
### Leave a parameter unchanged if it's not applicable for your search
token: ""                            API Token could be acquired from your dashboard
start_date : null                    Retrieve posts from this point onwards
end_date: null                       Retrieve posts no later than this date time
output_filename: null                The output csv file name for all the posts curated
search_terms: null                   "taco pizza bread" will retrieve posts including all of these words. "taco,pizza,bread" will retrive any of these words.
AND_terms: null                      Terms that must be included on top of the search terms 
NOT_terms: null                      Posts matching the search_terms above with these NOT_terms will not be included in the final search result.
lists: null                          Lists created in CT dashboard for grouping different accounts together to retrive the posts only from these lists.
accounts: null                       Similar to lists, but individual account ids.
page_admin_country: null             To filter posts whose page admins' countries match the countries provided. E.g "AU"  
history: false                       Set it to true to includes timestep data for growth of each post returned in terms of interaction counts.
togbq: false                         Set it to true if you like to store the result in Big Query(BQ)                                  
bq_credential: "xxx.json"            The credential file to push to a dataset in your BQ project
bq_table_id: "dataset_id.table_id "  The name of the dataset and table to store your data. Dataset must exist in Big Query. Table will be created on the fly.
```


### Using Crowd Subsequently
- Activate the environment `conda activate <your_env_name>`
- [Optional] If you like to have separate tables for list data in BigQuery `git checkout tableflattening` 
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
  1. crowd -c "myquery.yml"                        Run the query with params set in "myquery.yml"
  2. crowd -c "myquery.yml" -a                     Append to the csv file stated in myquery.yml file


Source code available at https://github.com/qut-dmrc/Crowd.git