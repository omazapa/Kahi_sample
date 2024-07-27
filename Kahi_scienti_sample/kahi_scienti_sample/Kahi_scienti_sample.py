from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed


class Kahi_scienti_sample(KahiBase):
    """
    Class to process the scienti database and extract works samples based on different criteria.
    With this class you can extract works based on authors, products, types, groups, institutions, 
    categories, custom queries and custom pipelines.

    """

    config = {}

    def __init__(self, config):
        """
        Initialize the Kahi_scienti_sample plugin.
        """
        self.config = config
        self.database_out_url = self.config["scienti_sample"]['database_out']["database_url"]
        self.database_out_name = self.config["scienti_sample"]['database_out']["database_name"]
        self.database_out_collection = self.config["scienti_sample"]['database_out']["collection_name"]
        self.database_out_drop_database = self.config["scienti_sample"]['database_out']["drop_database"]

        self.client = MongoClient(self.database_out_url)
        self.db = self.client[self.database_out_name]
        self.collection = self.db[self.database_out_collection]

        self.num_jobs = self.config["scienti_sample"]["num_jobs"] if "num_jobs" in self.config["scienti_sample"] else 1
        if self.database_out_drop_database:
            self.client.drop_database(self.database_out_name)
        self.dbs_in = []

        for db in self.config["scienti_sample"]['databases']:
            client_in = MongoClient(db["database_url"])
            db_in = client_in[db["database_name"]]
            collection_in = db_in[db["collection_name"]]

            self.dbs_in.append({"client": client_in, "db": db_in, "collection": collection_in})
        self.verbose = self.config["scienti_sample"]["verbose"] if "verbose" in self.config["scienti_sample"] else 1


    def process_products(self):
        """
        process products given the COD_RH and COD_PRODUCTO in the workflow configuration.
        """
        if "products" in self.config["scienti_sample"] and self.config["scienti_sample"]["products"]:
            product_ids = []
            for product in self.config["scienti_sample"]["products"]:
                product_ids.append(dict(product))
            if self.verbose > 0:
                print("INFO: Processing products: ", len(product_ids))
            for product_id in product_ids:
                for db in self.dbs_in:
                    work = db["collection"].find_one(product_id)
                    if work:
                        if self.collection.count_documents(product_id) == 0:
                            self.collection.insert_one(work)
                        else:
                            if self.verbose > 2:
                                print(f"INFO: Product {product_id} already exists in db {self.db.name} collection {self.collection.name}")
                    else:
                        if self.verbose > 2:
                            print(f"WARNING: Product {product_id} not found in the database {db['db'].name} collection {db["collection"].name}")
    def process_authors(self):
        """
        Process authors given the COD_RH in the workflow configuration.
        All the works of the author are saved in the output database.
        """
        if "authors" in self.config["scienti_sample"] and self.config["scienti_sample"]["authors"]:
            author_ids = self.config["scienti_sample"]["authors"]
            if self.verbose > 0:
                print("INFO: Processing authors: ", len(author_ids))
            for author_id in author_ids:
                for db in self.dbs_in:
                    if self.verbose > 0:
                        print(f"INFO: Found {db['collection'].count_documents({'COD_RH':author_id})} in db {db['db'].name} collection {db['collection'].name} for id   {author_id}")
                    works = db["collection"].find({"COD_RH":author_id})
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)
    def process_one_work(self,work):
        """
        Method to process one work and save it in the output database.
        Required for parallel processing.
        """
        work_id = {"COD_RH":work["COD_RH"], "COD_PRODUCTO":work["COD_PRODUCTO"]}
        if self.collection.count_documents(work_id) == 0:
            self.collection.insert_one(work)
        else:
            if self.verbose > 2:
                print(f"INFO: Work  {work_id} already exists in db {self.db.name} collection {self.collection.name} ")

    def process_types(self):
        """
        Method to process types given the types in the workflow configuration.
        An example of scienti type if for example the code 111 that means "Artículo en revista indexada".
        In the workflow configuration you can specify the types to process and it will save in the output 
        database all the works that match the type.
        """
        if "types" in self.config["scienti_sample"] and self.config["scienti_sample"]["types"]:
            type_ids = []
            for type_id in self.config["scienti_sample"]["types"]:
                type_ids.append(dict(type_id))
            if self.verbose > 0:
                print("INFO: Processing types: ", len(type_ids))
            for type_id in type_ids:
                for db in self.dbs_in:
                    if self.verbose > 0:
                        count = db["collection"].count_documents({"$or":[{"product_type":{"$elemMatch":type_id}},
                                                          {"product_type.product_type":{"$elemMatch":type_id}},
                                                          {"product_type.product_type.product_type":{"$elemMatch":type_id}}]})

                        print(f"INFO: Found {count} in db {db['db'].name} collection {db['collection'].name} for id   {type_id}")
                    

                    #there are three levels of nesting in the product_type field
                    works = db["collection"].find({"$or":[{"product_type":{"$elemMatch":type_id}},
                                                          {"product_type.product_type":{"$elemMatch":type_id}},
                                                          {"product_type.product_type.product_type":{"$elemMatch":type_id}}]})
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)
    def process_groups(self):
        """
        Method to process groups given the groups tag in the workflow configuration.
        Scienti research groups are associated with works and institutions,
        this method will save in the output database all the works that match the group.
        """
        if "groups" in self.config["scienti_sample"] and self.config["scienti_sample"]["groups"]:
            group_ids = self.config["scienti_sample"]["groups"]
            if self.verbose > 0:
                print("INFO: Processing groups: ", len(group_ids))
            for group_id in group_ids:
                for db in self.dbs_in:
                    if self.verbose > 0:

                        print(f"INFO: Found {db['collection'].count_documents({'group':{"$elemMatch":group_id}})} in db {db['db'].name} collection {db['collection'].name} for id   {group_id}")
                    works = db["collection"].find({'group':{"$elemMatch":group_id}})
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)

    def process_institutions(self):
        """
        Method to process institutions given the institutions tag in the workflow configuration.
        Scienti institutions are associated with works,
        this method will save in the output database all the works that match the institution.
        """
        if "institutions" in self.config["scienti_sample"] and self.config["scienti_sample"]["institutions"]:
            institution_ids = self.config["scienti_sample"]["institutions"]
            if self.verbose > 0:
                print("INFO: Processing institutions: ", len(institution_ids))
            for institution_id in institution_ids:
                for db in self.dbs_in:
                    if self.verbose > 0:
                        print(f"INFO: Found {db['collection'].count_documents({'institution':{"$elemMatch":institution_id}})} in db {db['db'].name} collection {db['collection'].name} for id   {institution_id}")
                    works = db["collection"].find({'institution':{"$elemMatch":institution_id}})
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)
    def process_custom_queries(self):
        """
        Method to process custom queries given the custom_queries tag in the workflow configuration.
        Custom queries are used to extract works based on custom criteria,
        this method will save in the output database all the works that match the custom query.
        """
        if "custom_queries" in self.config["scienti_sample"] and  self.config["scienti_sample"]["custom_queries"]:
            queries = self.config["scienti_sample"]["custom_queries"]
            if self.verbose > 0:
                print("INFO: Processing custom queries: ", len(queries))
            for query in queries:
                for db in self.dbs_in:
                    if self.verbose > 0:
                        print(f"INFO: Found {db['collection'].count_documents(query)} in db {db['db'].name} collection {db['collection'].name} for query   {query}")
                    works = db["collection"].find(query)
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)

    def process_custom_pipelines(self):
        """
        Method to process custom pipelines given the custom_pipelines tag in the workflow configuration.
        Custom pipelines are used to extract works based on custom criteria,
        this method will save in the output database all the works that match the custom pipeline
        """
        if "custom_pipelines" in self.config["scienti_sample"] and  self.config["scienti_sample"]["custom_pipelines"]:
            pipelines = self.config["scienti_sample"]["custom_pipelines"]
            if self.verbose > 0:
                print("INFO: Processing custom queries: ", len(pipelines))
            for pipeline in pipelines:
                for db in self.dbs_in:
                    works = list(db["collection"].aggregate(pipeline))
                    if self.verbose > 0:
                        print(f"INFO: Found {len(works)} in db {db['db'].name} collection {db['collection'].name} for query {pipeline}")
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)
    
    def process_categories(self):
        """
        Method to process categories given the categories tag in the workflow configuration.
        Categories are used to extract works based on categories,
        this method will save in the output database all the works that match the category
        """
        if "categories" in self.config["scienti_sample"] and self.config["scienti_sample"]["categories"]:
            category_ids = self.config["scienti_sample"]["categories"]
            if self.verbose > 0:
                print("INFO: Processing categories: ", len(category_ids))
            for category_id in category_ids:
                for db in self.dbs_in:
                    if self.verbose > 0:
                        print(f"INFO: Found {db['collection'].count_documents(category_id)} in db {db['db'].name} collection {db['collection'].name} for id   {category_id}")
                    works = db["collection"].find(category_id)
                    Parallel(n_jobs=self.num_jobs,backend="threading",verbose=10)(delayed(self.process_one_work)(work) for work in works)

    def run(self):
        self.process_authors()
        self.process_products()
        self.process_types()
        self.process_groups()
        self.process_institutions()
        self.process_custom_queries()
        self.process_custom_pipelines()
        self.process_categories()
        return 0