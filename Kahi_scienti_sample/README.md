<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scienti plugin 
Sample data generation for scienti database

# Description
This plugin allows to save samples given different scienti ids.

# Installation

## Dependencies
This required a scienti database in MongoDB already extracted with 
with https://github.com/colav/kaypacha

## Package

`pip install kahi_scienti_sample`


# Usage
This is an example of the work flow to the extraction,
take the section of the workflow you need to get your sample.

```.yaml
config:
  database_url: localhost
  log_database: kahi_saples_log
  log_collection: log
  profile: False
workflow:
  scienti_sample:
    verbose: 1
    num_jobs: 20 
    database_out:
      drop_database: True
      database_url: localhost
      database_name: scienti_sample
      collection_name: product
    databases:
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: product_udea
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: product_uec
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: product_unaula
      - database_url: localhost:27017
        database_name: scienti_111
        collection_name: product_univalle
    authors: 
      - "0000177733" #Diego Restrepo
      - "0001385569" # Claudia Marcela Velez
      - "0000536237" # Gabriel Jaime Velez Cuartas
    products:
      - COD_RH: "0000177733" #Diego Restrepo
        COD_PRODUCTO: "67"   #Radiative seesaw model:..
      - COD_RH: "0001385569" # Claudia Marcela Velez
        COD_PRODUCTO: "63"   #Addressing overuse of health..
      - COD_RH: "0000536237" # Gabriel Jaime Velez Cuartas
        COD_PRODUCTO: "239"  #Regional and global science:..
    types:
      - COD_TIPO_PRODUCTO: "111" #published article is specialized scientific journal
    groups:
      - NRO_ID_GRUPO: "00000000001682" #Grupo de Fenomenología de Interacciones Fundamentales
      - COD_ID_GRUPO: "COL0008423" #Grupo de Fenomenología de Interacciones Fundamentales (you can use alternative id)
    institutions:
      - COD_INST: "007300000887" #Universidad de Antioquia
      - TXT_NIT: "890980040" #Universidad de Antioquia (you can use nit id)
        TXT_DIGITO_VERIFICADOR: "8" # but verification code is required
    categories:
      - CAT_MED: 'ART-ART_A2' # measured category
      - SGL_CATEGORIA: "ART-ART_C" # general catergory
    custom_queries: # you have to build the mongodb query and it have to return always works
      - {"group.NRO_ID_GRUPO": "00000000001682", "product_type.COD_TIPO_PRODUCTO": "111"} # products 111 of  Gfif group
      - {"COD_RH": "0000536237","group":{"$exists":False}} # Gabriel Jaime Velez Cuartas products without group
    custom_pipelines: # you have to build the mongodb pipeline and it have to return always works
        # this is a pipeline that returns the products of co-authors that are in the group for products 111
        # and the co-authors has available COD_RH_REF
      - [{"$match": { 
            "group.NRO_ID_GRUPO": "00000000001682",
            "product_type.COD_TIPO_PRODUCTO": "111"
        }},
        {"$unwind": "$author_others"},
        {"$match": {
                "author_others.COD_RH_REF": {"$ne": None}
            }
        },
        {"$group": {
                "_id": None,
                "cod_rh_refs": { "$addToSet": "$author_others.COD_RH_REF" }
            }
        },
        {"$lookup": {
                "from": "product_udea",
                "let": { "refs": "$cod_rh_refs" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": [ "$COD_RH", "$$refs" ]
                            }
                        }
                    }
                ],
                "as": "matching_documents"
            }
        },
        {"$unwind": "$matching_documents"
        },
        {"$replaceRoot": { "newRoot": "$matching_documents" }
        }
        ]
        # simple pipeline that returns the products of Gabriel Jaime Velez Cuartas without group
      - [{"$match": {"COD_RH": "0000536237","group":{"$exists":False}}}]
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



