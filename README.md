### From your environment
```
cd anaconda
conda create --name "your_env"
activate <your_env>
git clone https://github.com/xueyingtan/Crowd.git
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
  --token           The API token associated with dashboard                                         [string] [required]
  --lists           A comma-separated list of the IDs of lists to search within.               [string] [default: null]
  --search_terms    Retrieve posts which contain the search terms                              [string] [default: null]
  --start_date      Filter posts ranging from start_date (e.g "2020-08-08")         [string] [default:end_date-365days]
  --end_date        Filter posts ranging to end_date (e.g "2020-08-08")                         [string] [default: now]
  --output_filename File to write/append to with csv extension only ('result.csv')  [string][default:<currentTime>.csv]
  --help

Examples:
  crowd "posts/search" --token="<API_TOKEN>"     Download all the available posts from posts/search endpoint
  --lists="123,456" --search-terms="kw1,kw2"     restricted to lists IDs 123 and 456 matching keywords kw1 or kw2

Source code available at https://github.com/xueyingtan/Crowd