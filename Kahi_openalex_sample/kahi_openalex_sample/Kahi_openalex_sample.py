from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed


class Kahi_openalex_sample(KahiBase):
    """
    Class to process the openalex database and extract works samples based on different criteria.
    With this class you can extract works based on authors, products, types, institutions custom
    queries and custom pipelines.

    """

    config = {}

    def __init__(self, config):
        """
        Initialize the Kahi_openalex_sample plugin.
        """
        self.config = config
        self.database_out_url = self.config["openalex_sample"]['database_out']["database_url"]
        self.database_out_name = self.config["openalex_sample"]['database_out']["database_name"]
        self.database_out_drop_database = self.config["openalex_sample"]['database_out']["drop_database"]

        self.database_collection_works = "works"
        self.database_collection_authors = "authors"
        self.database_collection_concepts = "concepts"
        self.database_collection_funders = "funders"
        self.database_collection_institutions = "institutions"
        self.database_collection_publishers = "publishers"
        self.database_collection_sources = "sources"

        self.client = MongoClient(self.database_out_url)
        self.db_out = self.client[self.database_out_name]
        self.collection_works_out = self.db_out[self.database_collection_works]

        self.num_jobs = self.config["openalex_sample"]["num_jobs"] if "num_jobs" in self.config["openalex_sample"] else 1
        if self.database_out_drop_database:
            self.client.drop_database(self.database_out_name)
        self.database_in_url = self.config["openalex_sample"]['database_in']["database_url"]
        self.database_in_name = self.config["openalex_sample"]['database_in']["database_name"]
        self.client_in = MongoClient(self.database_in_url)
        self.db_in = self.client_in[self.database_in_name]
        self.collection_in = self.db_in["works"]
        self.verbose = self.config["openalex_sample"]["verbose"] if "verbose" in self.config["openalex_sample"] else 1

    def process_works(self):
        """
        process works given the openalex id in the workflow configuration.
        """
        if "products" in self.config["openalex_sample"] and self.config["openalex_sample"]["products"]:
            product_ids = self.config["openalex_sample"]["products"]
            if self.verbose > 0:
                print("INFO: Processing products: ", len(product_ids))
            for product_id in product_ids:
                if not product_id.startswith("https://openalex.org/W"):
                    print("ERROR: Invalid product id: ", product_id)
                    raise Exception("Invalid product id: ", product_id)

                work = self.collection_in.find_one({"id": product_id})
                if work:
                    if self.collection_works_out.count_documents({"id": product_id}) == 0:
                        self.collection_works_out.insert_one(work)
                    else:
                        if self.verbose > 2:
                            print(
                                f"INFO: Product {product_id} already exists in db {self.db_out.name} collection {self.collection_works_out.name}")
                else:
                    if self.verbose > 2:
                        print(
                            f"WARNING: Product {product_id} not found in the database {self.db_out.name} collection {self.collection_in.name}")

    def process_authors(self):
        """
        Process authors given the id in the workflow configuration.
        All the works of the author are saved in the output database.
        """
        if "authors" in self.config["openalex_sample"] and self.config["openalex_sample"]["authors"]:
            author_ids = self.config["openalex_sample"]["authors"]
            if self.verbose > 0:
                print("INFO: Processing authors: ", len(author_ids))
            for author_id in author_ids:
                if not author_id.startswith("https://openalex.org/A"):
                    print("ERROR: Invalid author id: ", author_id)
                    raise Exception("Invalid author id: ", author_id)
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.collection_in.count_documents({"authorships.author.id": author_id})} in db {self.db_in.name} collection {self.collection_in.name} for id   {author_id}")
                works = self.collection_in.find(
                    {"authorships.author.id": author_id})
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_one_work(self, work):
        """
        Method to process one work and save it in the output database.
        Required for parallel processing.
        """
        work_id = {"id": work["id"]}
        if self.collection_works_out.count_documents(work_id) == 0:
            self.collection_works_out.insert_one(work)
        else:
            if self.verbose > 2:
                print(
                    f"INFO: Work  {work_id} already exists in db {self.db_out.name} collection {self.collection_works_out.name} ")

    def process_types(self):
        """
        Method to process types given the types in the workflow configuration.
        type: 'article' # options are : [( 'article', 'book', 'book-chapter', 'dataset', 'dissertation', 'editorial',
                                         # 'erratum', 'letter', 'other', 'paratext', 'peer-review', 'preprint', 'reference-entry', 'report', 'review', 'standard')]
        type_crossref: 'journal-article' # options are: [ 'book', 'book-chapter', 'book-part', 'book-series', 'book-set', 'component', 'dataset', 'dissertation', 'edited-book',
                                                  #       'journal', 'journal-article', 'journal-issue', 'journal-volume', 'monograph', 'other', 'peer-review', 'posted-content',
                                                  #        'proceedings', 'proceedings-article', 'proceedings-series', 'reference-book', 'reference-entry', 'report', 'report-series', 'standard']
        In the workflow configuration you can specify the types to process and it will save in the output.
        database all the works that match the type.
        """
        if "types" in self.config["openalex_sample"] and self.config["openalex_sample"]["types"]:
            type_ids = self.config["openalex_sample"]["types"]
            if self.verbose > 0:
                print("INFO: Processing types: ", len(type_ids))
            for type_id in type_ids:
                if self.verbose > 0:
                    count = self.collection_in.count_documents(type_id)

                    print(
                        f"INFO: Found {count} in db {self.db_in.name} collection {self.collection_in.name} for id   {type_id}")

                # there are three levels of nesting in the product_type field
                works = self.collection_in.find(type_id)
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_institutions(self):
        """
        Method to process institutions given the institutions tag in the workflow configuration.
        openalex institutions are associated with works,
        this method will save in the output database all the works that match the institution.
        """
        if "institutions" in self.config["openalex_sample"] and self.config["openalex_sample"]["institutions"]:
            institution_ids = self.config["openalex_sample"]["institutions"]
            if self.verbose > 0:
                print("INFO: Processing institutions: ", len(institution_ids))
            for institution_id in institution_ids:

                if not institution_id.startswith("https://openalex.org/I"):
                    print("ERROR: Invalid institution id: ", institution_id)
                    raise Exception("Invalid institution id: ", institution_id)
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.collection_in.count_documents({'authorships.institutions.id': institution_id})} in db {self.db_in.name} collection {self.collection_in.name} for id   {institution_id}")
                works = self.collection_in.find(
                    {'authorships.institutions.id': institution_id})
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_custom_queries(self):
        """
        Method to process custom queries given the custom_queries tag in the workflow configuration.
        Custom queries are used to extract works based on custom criteria,
        this method will save in the output database all the works that match the custom query.
        """
        if "custom_queries" in self.config["openalex_sample"] and self.config["openalex_sample"]["custom_queries"]:
            queries = self.config["openalex_sample"]["custom_queries"]
            if self.verbose > 0:
                print("INFO: Processing custom queries: ", len(queries))
            for query in queries:
                if self.verbose > 0:
                    print(
                        f"INFO: Found {self.collection_in.count_documents(query)} in db {self.db_in.name} collection {self.collection_in.name} for query   {query}")
                works = self.collection_in.find(query)
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def process_custom_pipelines(self):
        """
        Method to process custom pipelines given the custom_pipelines tag in the workflow configuration.
        Custom pipelines are used to extract works based on custom criteria,
        this method will save in the output database all the works that match the custom pipeline
        """
        if "custom_pipelines" in self.config["openalex_sample"] and self.config["openalex_sample"]["custom_pipelines"]:
            pipelines = self.config["openalex_sample"]["custom_pipelines"]
            if self.verbose > 0:
                print("INFO: Processing custom pipelines: ", len(pipelines))
            for pipeline in pipelines:
                works = self.collection_in.aggregate(pipeline)
                Parallel(n_jobs=self.num_jobs, backend="threading", verbose=10)(
                    delayed(self.process_one_work)(work) for work in works)

    def save_author(self, aid):
        """
        Utility function to save author in the output database.
        Check if the author exists in the output database else then it save it.

        Parameters:
        ----------
        aid: str
            author id to save in the output database.
        """
        author = self.db_in[self.database_collection_authors].find_one({
                                                                       "id": aid})
        if author is not None:
            self.db_out[self.database_collection_authors].insert_one(author)

    def post_process_authors(self):
        """
        This method saves the authors found in output works in the authors collection.
        """
        print(
            f"INFO: processing index from {self.db_in.name}.{self.database_collection_authors} ")
        self.db_in[self.database_collection_authors].create_index("id")
        pipeline = [
            {"$project": {"_id": 0, "authorships.author.id": 1}},
            {"$unwind": "$authorships"},
            {"$group": {"_id": None, "authors": {"$addToSet": "$authorships.author.id"}}},
            {"$unwind": "$authors"},
            {"$project": {"_id": 0}}
        ]

        authors_ids = self.db_out[self.database_collection_works].aggregate(
            pipeline)
        authors_ids = list(authors_ids)

        print(
            f"processing authors from {self.db_out.name}.{self.database_collection_works} to {self.db_out.name}.authors filtering from {self.db_in.name}.{self.database_collection_authors}")
        Parallel(n_jobs=self.num_jobs, verbose=10, backend="threading", batch_size=10)(
            delayed(self.save_author)(author["authors"]) for author in authors_ids)

    def post_process_concepts(self):
        """
        Method to post process concepts and save them in the output database.
        """
        # concepts
        print("INFO: processing concepts ")
        pipeline = [
            {"$match": {}},
            {"$out": {"db": self.db_out.name, "coll": self.database_collection_concepts}}
        ]
        self.db_in[self.database_collection_concepts].aggregate(pipeline)

    def post_process_funders(self):
        """
        Method to post process funders and save them in the output database.
        """
        # funders
        print("INFO: processing funders ")
        pipeline = [
            {"$match": {}},
            {"$out": {"db": self.db_out.name, "coll": self.database_collection_funders}}
        ]
        self.db_in[self.database_collection_funders].aggregate(pipeline)

    def post_process_institutions(self):
        """
        Method to post process institutions and save them in the output database.
        """
        # institutions
        print("INFO: processing institutions ")
        pipeline = [
            {"$match": {}},
            {"$out": {"db": self.db_out.name,
                      "coll": self.database_collection_institutions}}
        ]
        self.db_in[self.database_collection_institutions].aggregate(pipeline)

    def post_process_publishers(self):
        """
        Method to post process publishers and save them in the output database.
        """
        # publishers
        print("INFO: processing publishers ")
        pipeline = [
            {"$match": {}},
            {"$out": {"db": self.db_out.name, "coll": self.database_collection_publishers}}
        ]
        self.db_in[self.database_collection_publishers].aggregate(pipeline)

    def post_process_sources(self):
        """
        Method to post process sources and save them in the output database.
        """
        # sources
        print("INFO: processing sources ")
        pipeline = [
            {"$match": {}},
            {"$out": {"db": self.db_out.name, "coll": self.database_collection_sources}}
        ]
        self.db_in[self.database_collection_sources].aggregate(pipeline)

    def run(self):
        self.process_authors()
        self.process_works()
        self.process_types()
        self.process_institutions()
        self.process_custom_queries()
        self.process_custom_pipelines()
        self.post_process_authors()
        self.post_process_publishers()
        self.post_process_concepts()
        self.post_process_funders()
        self.post_process_institutions()
        self.post_process_sources()
        return 0
