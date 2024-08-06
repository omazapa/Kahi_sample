<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scienti plugin 
Sample data generation for scholar database

# Description
This plugin allows to save samples given different scholar ids.

# Installation

## Dependencies
This required a scholar database in MongoDB already extracted with 
with https://github.com/colav/Moai

## Package

`pip install kahi_scholar_sample`


# Usage
This is an example of the work flow to the extraction,
take the section of the workflow you need to get your sample.

```.yaml
config:
  database_url: localhost
  log_database: kahi_samples_log
  log_collection: log
  profile: False
workflow:
  scholar_sample:
    verbose: 1
    num_jobs: 20
    database_out:
      drop_database: True
      database_url: localhost
      database_name: scholar_sample
      collection_name: stage
    database:
      database_url: localhost:27017
      database_name: scholar_colombia_2024
      collection_name: stage
    authors:
      - "1sKULCoAAAAJ" # Diego Restrepo
      - "RuclEJkAAAAJ" # Claudia Marcela Velez
      - "HcAnZ0MAAAAJ" # Gabriel Jaime Velez Cuartas
    products:
      - cid: 'T5emQzegQC4J'
        doi: 'https://doi.org/10.1103/physrevd.79.013011' # Radiative seesaw model:..
      - cid: 'T8NdyUKnbCoJ'
        doi: 'https://doi.org/10.1186/s12961-018-0325-x' # Addressing overuse of health..
      - cid: 'oYypzOZJZsEJ'
        doi: 'https://doi.org/10.3145/epi.2016.ene.05' # Regional and global science:..
    types:
      - 'article'
      - 'book'
      - 'incollection'
      - 'inproceedings'
      - 'mastersthesis'
      - 'misc'
      - 'phdthesis'
      - 'techreport'
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/