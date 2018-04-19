import sys
import os
import argparse
import logging

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import *

reload(sys)
sys.setdefaultencoding('utf-8')

class AnalyzeJson(object):

    def __init__(self):

        # Create logger
        self.log = logging.getLogger(__name__)
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_formatter)
        self.log.addHandler(console_handler)

        # File for storing the extracted data files
        # ToDo: Include in configuration file when it is needed in various applications
        current_dir = os.path.dirname(os.path.realpath(__file__))
        self.output_path = os.path.join(current_dir, "extract_dir")

        # Define default value for number of returned lines
        self.number_of_returned_lines = 20

    def main(self, args):
        cmd_parser = self.parse_arguments(args)
        sqlc = self.get_spark_context()

        json_dict = self.get_jsons()

        if cmd_parser.number_of_lines:
            self.log.info("Displaying "+  str(cmd_parser.number_of_lines) +" number of lines.")
            self.number_of_returned_lines = cmd_parser.number_of_lines

        # ToDo: This should be optimized.
        #       Not manually creating for each query one case
        if cmd_parser.all_queries or cmd_parser.query1:
            self.query1(sqlc, json_dict)
        if cmd_parser.all_queries or cmd_parser.query2:
            self.query2(sqlc, json_dict)
        if cmd_parser.all_queries or cmd_parser.query3:
            self.query3(sqlc, json_dict)
        if cmd_parser.all_queries or cmd_parser.query4:
            self.query4(sqlc, json_dict)
        if cmd_parser.all_queries or cmd_parser.query5:
            self.query5(sqlc, json_dict)
        if not cmd_parser.all_queries and not cmd_parser.query5:
            self.log.error("No query was selected")

    def query1(self, sqlc, dict_json_df):
        ''' Query1 Most useful reviews for a business with at least 3 stars '''
        query_json_list = ["business", "review"]

        dict_json_df = self.load_json_files(sqlc, dict_json_df, query_json_list)

        df1 = dict_json_df["business"]
        df2 = dict_json_df["review"]
        df1_filter = df1.where(df1.stars > 3).select("business_id", "name", "city", "stars")
        df2_filter = df2.select("useful", "business_id")

        df1_filter.join(df2_filter, df1_filter.business_id == df2_filter.business_id) \
                .sort(("useful"), ascending=False) \
                .select("city", "name", "useful") \
                .show(self.number_of_returned_lines)

    def query2(self, sqlc, dict_json_df):
        ''' Query2: Most reviews by user for a city.'''
        query_json_list = ["business", "tip", "user"]

        dict_json_df = self.load_json_files(sqlc, dict_json_df, query_json_list)

        df1 = dict_json_df["business"]
        df2 = dict_json_df["tip"]
        df3 = dict_json_df["user"]
        df_new = df1.join(df2, df1.business_id == df2.business_id) \
            .select("city", "user_id")

        df_new.join(df3, df_new.user_id == df3.user_id) \
            .drop(df_new.user_id) \
            .select("city", "user_id", "review_count") \
            .groupBy("city", "user_id") \
            .agg(sum("review_count").alias("review_count")) \
            .sort("review_count", ascending=False) \
            .select("city", "user_id", "review_count") \
            .show(self.number_of_returned_lines)

    def query3(self, sqlc, dict_json_df):
        ''' Query3: Show businesses which have photos order by stars.'''
        query_json_list = ["business", "photos"]

        dict_json_df = self.load_json_files(sqlc, dict_json_df, query_json_list)

        df1 = dict_json_df["business"]
        df2 = dict_json_df["photos"]

        df1.join(df2, df1.business_id == df2.business_id) \
            .orderBy("stars", ascending=False) \
            .select("name", "city", "state", "stars") \
            .show(self.number_of_returned_lines)


    def query4(self, sqlc, dict_json_df):
        ' Query4: Most tips written by user joined Yelp before 2017'
        query_json_list = ["user", "tip"]
        dict_json_df = self.load_json_files(sqlc, dict_json_df, query_json_list)

        df1 = dict_json_df["user"]
        df2 = dict_json_df["tip"]
        df1_filter = df1.select("user_id") \
            .where(year("yelping_since") < 2017) \
            .sort("yelping_since", ascending=True)

        df1_filter.join(df2, df1_filter.user_id == df2.user_id) \
            .groupBy(df2.user_id) \
            .count() \
            .sort("count", ascending=False) \
            .select("user_id", "count") \
            .show(self.number_of_returned_lines)
    
    def query5(self, sqlc, dict_json_df):
        ''' Query5: Most reviews in a city '''
        query_json_list = ["business"]
        dict_json_df = self.load_json_files(sqlc, dict_json_df, query_json_list)

        df1 = dict_json_df["business"]
        df1.select("city", "review_count").groupBy("city") \
            .agg(sum("review_count").alias("review_count")) \
            .sort("review_count", ascending=False) \
            .show(self.number_of_returned_lines)

    def load_json_files(self, sqlc, json_dict, query_json_list):
        ''' Read JSON file with  '''
        dict_json_df = {}
        dict_json_files_load = self.is_json_available(json_dict, query_json_list)
        for json_name, json_path in dict_json_files_load.iteritems():
            dict_json_df[json_name] = sqlc.read.json(json_path)
        return dict_json_df

    def is_json_available(self, dict_available_json_files, list_need_json_files):
        ''' Check if Json file is available from the extraced  '''
        dict_json_files_load = {}

        for needed_json_files in list_need_json_files:
            found_json = False
            for available_json, available_json_file in dict_available_json_files.iteritems():
                if needed_json_files == available_json:
                    dict_json_files_load[available_json] = available_json_file
                    self.log.info("Analyze the following json: '"+ available_json +"'")
                    found_json = True
                    break
            if not found_json:
                self.log.error("Didn't found json: '"+ needed_json_files +"'")
                sys.exit(1)
        return dict_json_files_load

    def get_jsons(self):
        ''' Create a dictionary with name and path of all available json files '''
        if not os.path.isdir(self.output_path):
            self.log.error("Please extract json files first. Use unpack.py")
            sys.exit(1)
        
        dict_available_json_files = {}
        for root, _, files in os.walk(self.output_path, topdown=True):
            for json_file in files:
                abs_path = os.path.abspath(os.path.join(root, json_file))
                if os.path.isfile(abs_path) and ".json" in abs_path:
                    dict_available_json_files[json_file.split(".")[0]] = abs_path

        return dict_available_json_files 

    def get_spark_context(self):
        ''' Create Spark Context '''
        conf = SparkConf().setAppName("New Yorker - Yelp").setMaster("local[*]")
        sc = SparkContext(conf=conf)
        sqlc = SQLContext(sc)

        return sqlc

    def parse_arguments(self, args):
        ''' Read command line options '''

        cmd_parser = argparse.ArgumentParser(description='New Yorker Application')

        cmd_parser.add_argument(
                "--all-queries",
                action="store_true",
                help="Run all available queries") 
        cmd_parser.add_argument(
                "--query1",
                action="store_true",
                help="Query1: Most useful reviews for a business with at least 3 stars") 
        cmd_parser.add_argument(
                "--query2",
                action="store_true",
                help="Query2: Most reviews by user for a city.") 
        cmd_parser.add_argument(
                "--query3",
                action="store_true",
                help="Query3: Show businesses which have photos order by stars") 
        cmd_parser.add_argument(
                "--query4",
                action="store_true",
                help="Query4: Most tips written by user joined Yelp before 2017") 
        cmd_parser.add_argument(
                "--query5",
                action="store_true",
                help="Query5: Most reviews in a city") 
        cmd_parser.add_argument(
                "--number-of-lines",
                type=int,
                help="Specify how many rows should be returned (Default: 20)")

        return cmd_parser.parse_args(args)


if __name__ == '__main__':
    analyzeJson = AnalyzeJson()
    analyzeJson.main(sys.argv[1:])
