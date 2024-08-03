<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi openalex_sample plugin 
Sample data generation for openalex database.

# Description
Kahi_openalex_sample is a Python plugin designed to process and extract samples from the OpenAlex database. This tool provides a flexible framework for querying and manipulating data related to academic works, including authors, institutions, types, and custom queries or pipelines.

Key Features
* **Sample Extraction**: Extract works based on specific authors, products, types, institutions, custom queries, or pipelines.
* **Parallel Processing**: Leverage multi-core processing for efficient data handling using the joblib library.
* **Customizable**: Configuration options allow you to tailor the data extraction process to your needs, including verbosity levels for detailed logging.
* **Post-Processing**: Automatically handle related data entities like authors, concepts, funders, institutions, publishers, and sources, saving them into the output database.

# Installation

## Dependencies
This required a openalex database in MongoDB already extracted with 
with https://github.com/colav-playground/openalex_load

## Package
Write here how to install this plugin
usauly is 

`pip install kahi_openalex_sample`


# Usage
This is an example of the work flow to the extraction,
take the section of the workflow you need to get your sample.

```
config:
  database_url: localhost
  log_database: kahi_samples_log
  log_collection: log
  profile: False
workflow:
  openalex_sample:
    verbose: 1
    num_jobs: 20 
    database_out:
      drop_database: True
      database_url: localhost
      database_name: openalex_sample
      indeces: []
    database_in:
      database_url: localhost:27017
      database_name: openalexco
      indeces: ["id","authorships.author.id","authorships.countries"]
    authors: 
      - "https://openalex.org/A5049648126" #Diego Restrepo perfil 1
      - "https://openalex.org/A5005743365" #Diego Restrepo perfil 2
    products:
      - "https://openalex.org/W2028315718"
      - "https://openalex.org/W1580696544"
    types:
      - type: 'book' # options are : [( 'article', 'book', 'book-chapter', 'dataset', 'dissertation', 'editorial', 
                                         # 'erratum', 'letter', 'other', 'paratext', 'peer-review', 'preprint', 'reference-entry', 'report', 'review', 'standard')]
      - type_crossref: 'monograph' # options are: [ 'book', 'book-chapter', 'book-part', 'book-series', 'book-set', 'component', 'dataset', 'dissertation', 'edited-book', 
                                                  #       'journal', 'journal-article', 'journal-issue', 'journal-volume', 'monograph', 'other', 'peer-review', 'posted-content', 
                                                  #        'proceedings', 'proceedings-article', 'proceedings-series', 'reference-book', 'reference-entry', 'report', 'report-series', 'standard']
    institutions:
      - https://openalex.org/I35961687 #Universidad de Antioquia
      - https://openalex.org/I91732220 # Universidad del Valle
      - https://openalex.org/I4210150825 #Universidad Autónoma Latinoamericana"
      - https://openalex.org/I59986765 #Universidad Externado de Colombia 
    custom_queries: # you have to build the mongodb query and it have to return always works
      - {"type": 'article', "authorships.institutions.id": "https://openalex.org/I91732220"} # only articles from Universidad del Valle for instance.
    custom_pipelines: # you have to build the mongodb pipeline and it have to return always works
        # this is a pipeline that returns the products from colombian authors or institutions
      - [{"$project":{"_id":0}},{"$match":{"$or":[{"authorships.countries":"CO"},{"authorships.institutions.country_code":"CO"}]}}]
```
Those parameters are not really needed in the workflow file, it is just for illustration.


save the file with a name like workflow.yml and run it with

```
kahi_run --workflow workflow.yml
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



