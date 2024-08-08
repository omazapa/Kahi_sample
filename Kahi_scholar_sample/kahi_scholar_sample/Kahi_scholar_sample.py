from kahi_impactu_utils.Utils import doi_processor
from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed
import re


class Kahi_scholar_sample(KahiBase):
    """
    Class to process the Google scholar database and extract works samples based on different criteria.
    With this class you can extract works based on authors, products and types of works.

    """

    config = {}

    def __init__(self, config):
        """
        Initialize the Kahi_scholar_sample plugin.
        """
        self.config = config
        self.database_out_url = self.config["scholar_sample"]['database_out']["database_url"]
        self.database_out_name = self.config["scholar_sample"]['database_out']["database_name"]
        self.database_out_drop_database = self.config["scholar_sample"]['database_out']["drop_database"]

        self.client = MongoClient(self.database_out_url)
        self.db_out = self.client[self.database_out_name]

        self.num_jobs = self.config["scholar_sample"]["num_jobs"] if "num_jobs" in self.config["scholar_sample"] else 1
        if self.database_out_drop_database:
            self.client.drop_database(self.database_out_name)

        self.database_in_url = self.config["scholar_sample"]['database_in']["database_url"]
        self.database_in_name = self.config["scholar_sample"]['database_in']["database_name"]
        self.collection_in_name = self.config["scholar_sample"]['database_in']["collection_name"]
        self.client_in = MongoClient(self.database_in_url)
        self.db_in = self.client_in[self.database_in_name]
        self.col_in = self.db_in[self.collection_in_name]

        self.verbose = self.config["scholar_sample"]["verbose"] if "verbose" in self.config["scholar_sample"] else 1

    def process_one_work(self, work):
        """
        Method to process one work and save it in the output database.
        Required for parallel processing.
        """
        work_id = {"cid": work["cid"]}
        if self.db_out["stage"].count_documents(work_id) == 0:
            self.db_out["stage"].insert_one(work)
        else:
            if self.verbose > 2:
                print(
                    f"INFO: Work {work_id} already exists in db {self.db_out.name} collection {self.db_out['stage'].name}")

    def process_authors(self):
        """
        Process authors given the google scholar id in the workflow configuration.
        All the works of the author are saved in the output database.
        """
        if "authors" in self.config["scholar_sample"] and self.config["scholar_sample"]["authors"]:
            author_ids = self.config["scholar_sample"]["authors"]
            if self.verbose > 0:
                print("INFO: Processing authors: ", len(author_ids))
            for author_id in author_ids:
                pipeline = [
                    {'$match': {
                        '$expr': {'$gt': [{'$size': {'$filter': {'input': {'$objectToArray': '$profiles'}, 'as': 'profile', 'cond': {'$eq': ['$$profile.v', author_id]}}}}, 0]}}}]
                works = list(self.col_in.aggregate(pipeline))
                if self.verbose > 0:
                    print(
                        f"INFO: Found {len(works)} works in db {self.db_in.name} collection {self.col_in.name} for id {author_id}")
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_products(self):
        """
        process products given the cid or DOI in the workflow configuration.
        """
        if "products" in self.config["scholar_sample"] and self.config["scholar_sample"]["products"]:
            product_ids = []
            for product in self.config["scholar_sample"]["products"]:
                if "cid" in product and product["cid"]:
                    product_ids.append(product["cid"])
                elif "doi" in product and product["doi"]:
                    doi = doi_processor(product["doi"])
                    if doi:
                        doi = re.sub(r"https://doi.org/", "", doi)
                        product_ids.append(doi)
            for product_id in product_ids:
                work = self.col_in.find_one({"cid": product_id})
                if not work:
                    work = self.col_in.find_one({"doi": product_id})
                if work:
                    if self.verbose > 0:
                        print(
                            f"INFO: work found in db {self.db_in.name} collection {self.col_in.name} for id {product_id}")
                    self.db_out["stage"].insert_one(work)

    def process_types(self):
        """
        Process works types given the in the workflow configuration.
        """
        if "types" in self.config["scholar_sample"] and self.config["scholar_sample"]["types"]:
            for type_ in self.config["scholar_sample"]["types"]:
                works = list(self.col_in.find({"bibtex": {"$regex": f"^@{type_}*", "$options": "i"}}))
                if len(works) > 0:
                    if self.verbose > 0:
                        print("INFO: Processing {} works of type: {}".format(len(works), type_))
                    Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                        delayed(self.process_one_work)(work) for work in works)

    def run(self):
        self.process_authors()
        self.process_products()
        self.process_types()
        return 0
