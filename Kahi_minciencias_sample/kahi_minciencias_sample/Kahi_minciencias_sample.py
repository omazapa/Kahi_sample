from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed
import re


class Kahi_minciencias_sample(KahiBase):
    """
    Class to process the minciencias database and extract works samples based on different criteria.
    With this class you can extract works based on authors, products, types, groups, institutions, 
    categories, custom queries and custom pipelines.

    """

    config = {}

    def __init__(self, config):
        """
        Initialize the Kahi_minciencias_sample plugin.
        """
        self.config = config
        self.database_out_url = self.config["minciencias_sample"]['database_out']["database_url"]
        self.database_out_name = self.config["minciencias_sample"]['database_out']["database_name"]
        self.database_out_drop_database = self.config["minciencias_sample"]['database_out']["drop_database"]

        self.client = MongoClient(self.database_out_url)
        self.db_out = self.client[self.database_out_name]

        self.num_jobs = self.config["minciencias_sample"]["num_jobs"] if "num_jobs" in self.config["minciencias_sample"] else 1
        if self.database_out_drop_database:
            self.client.drop_database(self.database_out_name)
        self.database_in_url = self.config["minciencias_sample"]['database_in']["database_url"]
        self.database_in_name = self.config["minciencias_sample"]['database_in']["database_name"]
        self.client_in = MongoClient(self.database_in_url)
        self.db_in = self.client_in[self.database_in_name]
        self.cols_out = {}
        for col in self.config["minciencias_sample"]['database_out']["collection_names"]:
            key = str(next(iter(col.keys())))
            value = str(next(iter(col.values())))
            self.cols_out[key] = self.db_out[value]

        self.cols_in = {}
        for col in self.config["minciencias_sample"]['database_in']["collection_names"]:
            key = str(next(iter(col.keys())))
            value = str(next(iter(col.values())))
            self.cols_in[key] = self.db_in[value]
        self.verbose = self.config["minciencias_sample"]["verbose"] if "verbose" in self.config["minciencias_sample"] else 1

    def process_products(self):
        """
        process products given the COD_RH and COD_PRODUCTO in the workflow configuration.
        """
        if "products" in self.config["minciencias_sample"] and self.config["minciencias_sample"]["products"]:
            product_ids = []
            for product in self.config["minciencias_sample"]["products"]:
                regex = re.compile(product)
                product_ids.append(regex)
            if self.verbose > 0:
                print("INFO: Processing products regex: ", len(product_ids))
            total_found=0
            for product_id in product_ids:
                found=0
                work_cursor = self.cols_in["gruplac_production"].find({"id_producto_pd":{"$regex": product_id}})
                for work in work_cursor:
                    if self.cols_out["gruplac_production"].count_documents(work) == 0:
                        found+=1
                        total_found+=1
                        self.cols_out["gruplac_production"].insert_one(work)
                    else:
                        if self.verbose > 2:
                            print(
                                f"INFO: Product {product_id.pattern} already exists in db {self.db_out.name} collection {self.cols_out["gruplac_production_data"].name}")
                print(f"INFO: Found {found} works for product {product_id.pattern}")
            print(f"INFO: Found {total_found} works in total")

    def run(self):
        self.process_products()
        # return 0
