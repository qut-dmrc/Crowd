### From your environment
```
cd anaconda
conda create --name "your_env"
activate <your_env>
git clone https://github.com/qut-dmrc/Crowd.git
cd Crowd
pip install -r requirements.txt
pip install -e .
```

## Usage

### Command Line
```
$ Crowd
Usage: crowd <endpoint> [OPTIONS]

Commands:
  crowd "post/search" [OPTIONS]      searches the entire, cross-platform CrowdTangle system of posts

OPTIONS
  -token/--token       The API token associated with dashboard                                         [string] [required]
  -ls/--lists          A comma-separated list of the IDs of lists to search within.               [string] [default: null]
  -s/--search_terms    Retrieve posts which contain the search terms                              [string] [default: null]
  -sdate/--start_date  Filter posts ranging from start_date (e.g "2020-08-08")         [string] [default:end_date-365days]
  sedate/--end_date    Filter posts ranging to end_date (e.g "2020-08-08")                         [string] [default: now]
  --output_filename    File to write/append to with csv extension only ('result.csv')  [string][default:<currentTime>.csv]
  -off/--offset        Posts offset for pagination purpose                                               [int][default: 0] 
  -log/--log           Turning on logging option                                                    [bool][default: False] 
  --help

Examples:
  crowd "posts/search" -token="<API_TOKEN>"        Download all the available posts from posts/search endpoint
  -ls="123,456" -s="kw1,kw2"                       Restricted to lists IDs 123 and 456 matching keywords kw1 or kw2
  -off=500                                         Offset 500 to access page 6 (each page returns maximum 100 posts)
  -sdate="2020-06-01" -edate="2020-08-20"          Start and end dates
  -log                                             Turn on logging

Source code available at https://github.com/qut-dmrc/Crowd.git