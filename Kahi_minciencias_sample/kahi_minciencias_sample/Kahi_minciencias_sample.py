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

    def process_authors(self):
        """
        Process authors given the COD_RH in the workflow configuration.
        All the works of the author are saved in the output database.
        """
        if "authors" in self.config["minciencias_sample"] and self.config["minciencias_sample"]["authors"]:
            author_ids = self.config["minciencias_sample"]["authors"]
            if self.verbose > 0:
                print("INFO: Processing authors: ", len(author_ids))
            for author_id in author_ids:
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.cols_in['gruplac_production'].count_documents({'id_persona_pd': author_id})} in db {self.db_in.name} collection {self.cols_in['gruplac_production'].name} for id   {author_id}")  # noqa: E501
                works = self.cols_in["gruplac_production"].find(
                    {"id_persona_pd": author_id})
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_one_work(self, work):
        """
        Method to process one work and save it in the output database.
        Required for parallel processing.
        """
        work_id = {"id_producto_pd": work["id_producto_pd"]}
        if self.cols_out["gruplac_production"].count_documents(work_id) == 0:
            self.cols_out["gruplac_production"].insert_one(work)
        else:
            if self.verbose > 2:
                print(
                    f"INFO: Work {work_id} already exists in db {self.db_out.name} collection {self.cols_out['gruplac_production'].name}")

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
            total_found = 0
            for product_id in product_ids:
                found = 0
                work_cursor = self.cols_in["gruplac_production"].find(
                    {"id_producto_pd": {"$regex": product_id}})
                for work in work_cursor:
                    if self.cols_out["gruplac_production"].count_documents(work) == 0:
                        found += 1
                        total_found += 1
                        self.cols_out["gruplac_production"].insert_one(work)
                    else:
                        if self.verbose > 2:
                            print(
                                f"INFO: Product {product_id.pattern} already exists in db {self.db_out.name} collection {self.cols_out['gruplac_production_data'].name}")
                print(
                    f"INFO: Found {found} works for product {product_id.pattern}")
            print(f"INFO: Found {total_found} works in total")

    def process_groups(self):
        """
        Process groups given the COD_GRUPO in the workflow configuration.
        """
        if "groups" in self.config["minciencias_sample"] and self.config["minciencias_sample"]["groups"]:
            group_ids = self.config["minciencias_sample"]["groups"]
            if self.verbose > 0:
                print("INFO: Processing groups: ", len(group_ids))
            for group_id in group_ids:
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.cols_in['gruplac_production'].count_documents({'cod_grupo_gr': group_id})} in db {self.db_in.name} collection {self.cols_in['gruplac_production'].name} for id {group_id}")  # noqa: E501
                works = self.cols_in["gruplac_production"].find(
                    {"cod_grupo_gr": group_id})
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_categories(self):
        """
        Process categories given the COD_CATEGORY in the workflow configuration.
        """
        if "categories" in self.config["minciencias_sample"] and self.config["minciencias_sample"]["categories"]:
            category_ids = []
            for cat in self.config["minciencias_sample"]["categories"]:
                regex = re.compile(cat)
                category_ids.append(regex)
            if self.verbose > 0:
                print("INFO: Processing categories: ", len(category_ids))
            for category_id in category_ids:
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.cols_in['gruplac_production'].count_documents({'id_tipo_pd_med': category_id})} in db {self.db_in.name} collection {self.cols_in['gruplac_production'].name} for id {category_id.pattern}")  # noqa: E501
                works = self.cols_in["gruplac_production"].find(
                    {"id_tipo_pd_med": category_id})
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_custom_queries(self):
        """
        Process custom queries in the workflow configuration.
        """
        if "custom_queries" in self.config["minciencias_sample"] and self.config["minciencias_sample"]["custom_queries"]:
            queries = self.config["minciencias_sample"]["custom_queries"]
            if self.verbose > 0:
                print("INFO: Processing custom queries: ", len(queries))
            for query in queries:
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.cols_in['gruplac_production'].count_documents(query)} in db {self.db_in.name} collection {self.cols_in['gruplac_production'].name} for query {query}")
                works = self.cols_in["gruplac_production"].find(query)
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_custom_pipelines(self):
        """
        Process custom pipelines in the workflow configuration.
        """
        if "custom_pipelines" in self.config["minciencias_sample"] and self.config["minciencias_sample"]["custom_pipelines"]:
            pipelines = self.config["minciencias_sample"]["custom_pipelines"]
            if self.verbose > 0:
                print("INFO: Processing custom pipelines: ", len(pipelines))
            for pipeline in pipelines:
                works = self.cols_in["gruplac_production"].aggregate(pipeline)
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_cvlac_stage(self):
        """
        Process cvlac stage in the workflow configuration.
        """
        person_ids = self.cols_out["gruplac_production"].distinct(
            "id_persona_pd")
        if self.verbose > 0:
            print(
                f"INFO: Processing cvlac stage, found {len(person_ids)} unique persons: ")
        for person_id in person_ids:
            profile = self.cols_in["cvlac_stage"].find_one(
                {"id_persona_pr": person_id})
            if profile:
                if self.cols_out["cvlac_stage"].count_documents({"id_persona_pr": person_id}) == 0:
                    self.cols_out["cvlac_stage"].insert_one(profile)
                else:
                    if self.verbose > 2:
                        print(
                            f"INFO: Profile {person_id} already exists in db {self.db_out.name} collection {self.cols_out['cvlac_stage'].name}")
            else:
                print(
                    f"INFO: Profile {person_id} not found in cvlac_stage, looking in {self.cols_out['cvlac_stage_private'].name}")

                profile = self.cols_in["cvlac_stage_private"].find_one(
                    {"id_persona_pr": person_id})
                if profile:
                    if self.cols_out["cvlac_stage_private"].count_documents({"id_persona_pr": person_id}) == 0:
                        self.cols_out["cvlac_stage_private"].insert_one(
                            profile)
                    else:
                        if self.verbose > 2:
                            print(
                                f"INFO: Profile {person_id} already exists in db {self.db_out.name} collection {self.cols_out['cvlac_stage_private'].name}")
                else:
                    print(
                        f"INFO: Profile {person_id} not found in cvlac_stage_private.")

    def process_gruplac_groups(self):
        """
        Process gruplac groups in the workflow configuration.
        """
        group_ids = self.cols_out["gruplac_production"].distinct(
            "cod_grupo_gr")
        if self.verbose > 0:
            print(
                f"INFO: Processing gruplac groups, found {len(group_ids)} unique groups")
        for group_id in group_ids:
            group = self.cols_in["gruplac_groups"].find_one(
                {"cod_grupo_gr": group_id})
            if group:
                if self.cols_out["gruplac_groups"].count_documents({"cod_grupo_gr": group_id}) == 0:
                    self.cols_out["gruplac_groups"].insert_one(group)
                else:
                    if self.verbose > 2:
                        print(
                            f"INFO: Group {group_id} already exists in db {self.db_out.name} collection {self.cols_out['gruplac_groups'].name}")
            else:
                print(f"INFO: Group {group_id} not found in gruplac_groups.")

    def run(self):
        self.process_authors()
        self.process_products()
        self.process_groups()
        self.process_categories()
        self.process_custom_queries()
        self.process_custom_pipelines()
        self.process_cvlac_stage()
        self.process_gruplac_groups()
        return 0
