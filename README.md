### From your environment
```
cd anaconda
conda create --name "your_env" python=3.8
activate <your_env>
git clone https://github.com/qut-dmrc/Crowd.git
cd Crowd
pip install -r requirements.txt
pip install -e .
<edit default_query.yml file for default query/create a new yaml file adhering to the structure as in default_query.yml>
<Refer to different yaml file for different endpoints>
```

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
  2. crowd -config "myquery.yml"                   Run the query with params set in "myquery.yml"


Source code available at https://github.com/qut-dmrc/Crowd.git