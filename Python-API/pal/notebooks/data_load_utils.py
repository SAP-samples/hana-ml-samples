# -*- coding: utf-8 -*-
"""
Created on Mon May 07 11:22:07 2018

"""

try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import csv
import logging
import random
import math

class Settings:
    settings = None
    user = None
    @staticmethod
    def load_config(config_file):
            Settings.settings = configparser.ConfigParser()
            Settings.settings.read(config_file)
            try:
                url = Settings.settings.get("hana", "url")
            except:
                url = "unknown"
            try:
                port = Settings.settings.getint("hana", "port")
            except:
                port = 39015
            try:
                pwd = Settings.settings.get("hana", "passwd")
            except:
                pwd='pydevtest'
            try:
                Settings.user = Settings.settings.get("hana", "user")
            except:
                Settings.user = "PYDEVTEST"
            Settings._init_logger()
            return url, port, Settings.user, pwd

    @staticmethod
    def _set_log_level(logger, level):
        if level == 'info':
            logger.setLevel(logging.INFO)
        else:
            if level == 'warn':
                logger.setLevel(logging.WARN)
            else:
                if level == 'debug':
                    logger.setLevel(logging.DEBUG)
                else:
                    logger.setLevel(logging.ERROR)

    @staticmethod
    def _init_logger():
        logging.basicConfig()
        for c in ["hana_ml.ml_base", 'hana_ml.dataframe', 'hana_ml.algorithms.pal']:
            try:
                level = Settings.settings.get("logging", c)
            except:
                level = "error"
            logger = logging.getLogger(c)
            Settings._set_log_level(logger, level.lower())  #logger.setLevel(logging.INFO)
        #logger.addHandler(logging.NullHandler())

class DataSets:
    @staticmethod
    def _table_exists(connection, schema, table):
        sql = "SELECT COUNT(*) from TABLES WHERE SCHEMA_NAME='{0}' AND TABLE_NAME='{1}'".format(schema, table)
        with connection.connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            count = result[0][0]
            if count == 1:
                return True
        return False

    @staticmethod
    def _row_count(connection, schema, table):
        sql = "SELECT COUNT(*) from {0}.{1}".format(schema, table)
        count = 0
        with connection.connection.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            count = result[0][0]
        return count

    @staticmethod
    def _drop_and_create_table(connection, table_name, cols):
            sql = "DROP TABLE " + table_name
            try:
                with connection.connection.cursor() as cur:
                    #print(sql)
                    cur.execute(sql)
            except:
                print("Drop unsuccessful")
                pass

            sql = 'CREATE COLUMN TABLE ' + table_name + cols
            with connection.connection.cursor() as cur:
                print('Creating table {}'.format(table_name))
                #print(sql)
                cur.execute(sql)

    @staticmethod
    def _load_data(connection, table_descriptions, cols, inlist, batch_size=10000):
        for k,v in table_descriptions.items():
            DataSets._drop_and_create_table(connection, v[0], cols)
            with open(v[1], 'r') as my_file:
                reader = csv.reader(my_file, delimiter=',')
                data = list(reader)
                sql = 'insert into ' + v[0] + inlist
                #print(sql)
                print('Rows to load is {0}'.format(len(data)))
                count = len(data)
                # Chunk up the inserts
                chunk_size = batch_size
                chunks = int(count/chunk_size) + 2
                for chunk in range(chunks+1):
                    chunk_location = chunk*chunk_size
                    data_chunk= data[chunk_location:chunk_location+chunk_size]
                    #print(data_chunk)
                    if len(data_chunk) > 0:
                        with connection.connection.cursor() as cur:
                            rows_inserted = cur.executemany(sql, data_chunk)
                            print ("Rows inserted into %s in chunk %i: %s" % (v[0], chunk, len(rows_inserted)))
                #with connection.connection.cursor() as cur:
                #    rows_inserted = cur.executemany(sql, data)
                #    print ("Rows inserted into %s: %s" % (v[0], len(rows_inserted)))

    @staticmethod
    def _load(connection, schema, tables, table_descriptions, cols, inlist, batch_size=10000, force=False):
        existing_ones = [tbl for tbl in tables if DataSets._table_exists(connection, schema, tbl)]
        if force or len(existing_ones) != len(tables):
            DataSets._load_data(connection, table_descriptions, cols, inlist, batch_size=batch_size)
            print('Loaded new data')
        else:
            row_counts = [DataSets._row_count(connection, schema, tbl) for tbl in tables]
            print('Already loaded for \n{0} \nwith counts\n{1}'.format(tables, row_counts))
        return tuple(tables)

    @staticmethod
    def load_bank_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "DBM2_RFULL_TBL"
        training_tbl = "DBM2_RTRAINING_TBL"
        validation_tbl = "DBM2_RVALIDATION_TBL"
        test_tbl = "DBM2_RTEST_TBL"
        tables = [full_tbl, training_tbl, validation_tbl, test_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                ID INTEGER generated by default as identity,\
                AGE INTEGER,\
                JOB VARCHAR(256),\
                MARITAL VARCHAR(100),\
                EDUCATION VARCHAR(256),\
                DBM_DEFAULT VARCHAR(100),\
                HOUSING VARCHAR(100),\
                LOAN VARCHAR(100),\
                CONTACT VARCHAR(100),\
                DBM_MONTH VARCHAR(100),\
                DAY_OF_WEEK VARCHAR(100),\
                DURATION DOUBLE,\
                CAMPAIGN INTEGER,\
                PDAYS INTEGER,\
                PREVIOUS INTEGER,\
                POUTCOME VARCHAR(100),\
                EMP_VAR_RATE DOUBLE,\
                CONS_PRICE_IDX DOUBLE,\
                CONS_CONF_IDX DOUBLE,\
                EURIBOR3M DOUBLE,\
                NREMPLOYED INTEGER,\
                LABEL VARCHAR(10)\
                )'
        inlist = '("ID", "AGE", "JOB", "MARITAL", "EDUCATION",\
                    "DBM_DEFAULT", "HOUSING", "LOAN", "CONTACT", "DBM_MONTH", "DAY_OF_WEEK", \
                    "DURATION", "CAMPAIGN", "PDAYS", "PREVIOUS", "POUTCOME", \
                    "EMP_VAR_RATE", "CONS_PRICE_IDX", "CONS_CONF_IDX", "EURIBOR3M", \
                    "NREMPLOYED", "LABEL") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/bank-additional-full.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_titanic_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "TITANIC_FULL_TBL"
        training_tbl = "TITANIC_TRAIN_TBL"
        validation_tbl = "TITANIC_VALIDATION_TBL"
        test_tbl = "TITANIC_TEST_TBL"
        tables = [full_tbl, training_tbl, validation_tbl, test_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                PASSENGER_ID INTEGER,\
                PCLASS INTEGER,\
                NAME NVARCHAR(128),\
                SEX NVARCHAR(6),\
                AGE DOUBLE,\
                SIBSP INTEGER,\
                PARCH INTEGER,\
                TICKET NVARCHAR(18),\
                FARE DOUBLE,\
                CABIN NVARCHAR(15),\
                EMBARKED NVARCHAR(1),\
                SURVIVED INTEGER\
                )'
        inlist = '("PASSENGER_ID", "PCLASS", "NAME", "SEX", "AGE",\
                    "SIBSP", "PARCH", "TICKET", "FARE", "CABIN", "EMBARKED",\
                    "SURVIVED") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        """
        existing_ones = [tbl for tbl in tables if DataSets._table_exists(connection, schema, tbl)]
        if len(existing_ones) != len(tables):
            DataSets._load_data(connection, table_descriptions, cols, inlist)
            print('Loaded new data')
        else:
            row_counts = [DataSets._row_count(connection, schema, tbl) for tbl in tables]
            print('Already loaded for \n{0} \nwith counts {1}'.format(tables, row_counts))
        #return full_tbl, training_tbl, validation_tbl, test_tbl
        return tuple(tables)
        """
        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/titanic-full.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)


    @staticmethod
    def load_walmart_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        training_tbl = "WALMART_TRAIN_TBL"
        #test_tbl = "WALMART_TEST_TBL"
        tables = [training_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                ITEM_IDENTIFIER NVARCHAR(10),\
                ITEM_WEIGHT DOUBLE,\
                ITEM_FAT_CONTENT NVARCHAR(10),\
                ITEM_VISIBILITY  DOUBLE,\
                ITEM_TYPE NVARCHAR(30),\
                ITEM_MRP DOUBLE,\
                OUTLET_IDENTIFIER NVARCHAR(7),\
                OUTLET_ESTABLISHMENT_YEAR INTEGER,\
                OUTLET_SIZE NVARCHAR(7),\
                OUTLET_LOCATION_IDENTIFIER NVARCHAR(7),\
                OUTLET_TYPE NVARCHAR(20),\
                ITEM_OUTLET_SALES DOUBLE\
                )'
        inlist = '("ITEM_IDENTIFIER", "ITEM_WEIGHT", "ITEM_FAT_CONTENT", \
                    "ITEM_VISIBILITY", "ITEM_TYPE",\
                    "ITEM_MRP", "OUTLET_IDENTIFIER", "OUTLET_ESTABLISHMENT_YEAR",\
                    "OUTLET_SIZE", "OUTLET_LOCATION_IDENTIFIER", "OUTLET_TYPE",\
                    "ITEM_OUTLET_SALES") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)'

        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/walmart-train.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_black_friday_sales_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "BLACK_FRIDAY_SALES_TBL"
        tables = [full_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                USER_ID INTEGER,\
                PRODUCT_ID NVARCHAR(32),\
                GENDER NVARCHAR(1),\
                AGE NVARCHAR(32),\
                OCCUPATION INTEGER,\
                CITY_CATEGORY NVARCHAR(8),\
                STAY_IN_CURRENT_CITY_YEARS NVARCHAR(8),\
                MARITAL_STATUS INTEGER,\
                PRODUCT_CATEGORY_1 INTEGER,\
                PRODUCT_CATEGORY_2 INTEGER,\
                PRODUCT_CATEGORY_3 INTEGER,\
                PURCHASE DOUBLE\
                )'
        inlist = '("USER_ID", "PRODUCT_ID", "GENDER", "AGE",\
                    "OCCUPATION", "CITY_CATEGORY", "STAY_IN_CURRENT_CITY_YEARS",\
                    "MARITAL_STATUS", "PRODUCT_CATEGORY_1", "PRODUCT_CATEGORY_2",\
                    "PRODUCT_CATEGORY_3", "PURCHASE") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/black-friday-sales-full.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_iris_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "IRIS_DATA_FULL_TBL"
        training_tbl = "IRIS_DATA_TRAIN_TBL"
        validation_tbl = "IRIS_DATA_VALIDATION_TBL"
        test_tbl = "IRIS_DATA_TEST_TBL"
        tables = [full_tbl, training_tbl, validation_tbl, test_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
        ID INTEGER generated by default as identity,\
        SEPALLENGTHCM DOUBLE,\
        SEPALWIDTHCM  DOUBLE,\
        PETALLENGTHCM DOUBLE,\
        PETALWIDTHCM  DOUBLE,\
        SPECIES       NVARCHAR(15)\
    )'
        inlist = '("SEPALLENGTHCM", "SEPALWIDTHCM", "PETALLENGTHCM", "PETALWIDTHCM", "SPECIES")\
                    VALUES (?, ?, ?, ?, ?)'
        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/iris.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_boston_housing_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "BOSTON_HOUSING_PRICES"
        training_tbl = "BOSTON_HOUSING_PRICES_TRAINING"
        validation_tbl = "BOSTON_HOUSING_PRICES_VALIDATION"
        test_tbl = "BOSTON_HOUSING_PRICES_TEST"
        tables = [full_tbl, training_tbl, validation_tbl, test_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
        "CRIM" DECIMAL(12,5) CS_FIXED,\
        "ZN" DECIMAL(7,3) CS_FIXED,\
        "INDUS" DECIMAL(7,2) CS_FIXED,\
        "CHAS" SMALLINT CS_INT, "NOX" DECIMAL(10,4) CS_FIXED,\
        "RM" DECIMAL(8,3) CS_FIXED,\
        "AGE" DECIMAL(7,3) CS_FIXED,\
        "DIS" DECIMAL(11,4) CS_FIXED,\
        "RAD" TINYINT CS_INT,\
        "TAX" SMALLINT CS_INT,\
        "PTRATIO" DECIMAL(6,2) CS_FIXED,\
        "BLACK" DECIMAL(9,3) CS_FIXED,\
        "LSTAT" DECIMAL(7,2) CS_FIXED,\
        "MEDV" DECIMAL(6,2) CS_FIXED,\
        "ID" INTEGER\
        )'
        inlist = '("CRIM", "ZN", "INDUS", "CHAS",\
                    "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "BLACK", "LSTAT", "MEDV", "ID") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/boston-house-prices.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_flight_delays_1m(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "FLIGHT_DELAYS_1M"
        tables = [full_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
            ID BIGINT ,\
            YEAR INTEGER ,\
            MONTH INTEGER ,\
            DAYOFMONTH INTEGER ,\
            DAYOFWEEK INTEGER ,\
            DEPTIME INTEGER ,\
            CRSDEPTIME INTEGER ,\
            ARRTIME INTEGER ,\
            CRSARRTIME INTEGER ,\
            UNIQUECARRIER VARCHAR(5),\
            FLIGHTNUM INTEGER ,\
            TAILNUM VARCHAR(8),\
            ACTUALELAPSEDTIME INTEGER ,\
            CRSELAPSEDTIME INTEGER ,\
            AIRTIME INTEGER ,\
            ARRDELAY INTEGER ,\
            DEPDELAY INTEGER ,\
            ORIGIN VARCHAR(3),\
            DEST VARCHAR(3),\
            DISTANCE INTEGER ,\
            ISARRDELAYEDLABEL VARCHAR(1),\
            ISARRDELAYED INTEGER ,\
            ISDEPDELAYEDLABEL VARCHAR(1),\
            ISDEPDELAYED INTEGER \
                )'
        inlist = '("ID", "YEAR", "MONTH", "DAYOFMONTH", "DAYOFWEEK", "DEPTIME", \
                    "CRSDEPTIME", "ARRTIME", "CRSARRTIME", "UNIQUECARRIER", \
                    "FLIGHTNUM", "TAILNUM", "ACTUALELAPSEDTIME", "CRSELAPSEDTIME", \
                    "AIRTIME", "ARRDELAY", "DEPDELAY", "ORIGIN", "DEST", \
                    "DISTANCE", "ISARRDELAYEDLABEL", "ISARRDELAYED", \
                    "ISDEPDELAYEDLABEL", "ISDEPDELAYED") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/flight-delays-1m-full.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_flight_data(connection, schema=None, batch_size=10000, force=False, train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "FLIGHT_DATA_FULL_TBL"
        training_tbl = "FLIGHT_DATA_TRAIN_TBL"
        validation_tbl = "FLIGHT_DATA_VALIDATION_TBL"
        test_tbl = "FLIGHT_DATA_TEST_TBL"
        tables = [full_tbl, training_tbl, validation_tbl, test_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                YEAR INTEGER DEFAULT NULL,\
                MONTH INTEGER NULL,\
                DAY INTEGER NULL,\
                DAY_OF_WEEK INTEGER NULL,\
                AIRLINE VARCHAR(10),\
                FLIGHT_NUMBER VARCHAR(10),\
                TAIL_NUMBER VARCHAR(10),\
                ORIGIN_AIRPORT VARCHAR(10),\
                DESTINATION_AIRPORT VARCHAR(100),\
                SCHEDULED_DEPARTURE VARCHAR(10),\
                DEPARTURE_TIME VARCHAR(10),\
                DEPARTURE_DELAY INTEGER NULL,\
                TAXI_OUT VARCHAR(10),\
                WHEELS_OFF VARCHAR(10),\
                SCHEDULED_TIME VARCHAR(10),\
                ELAPSED_TIME VARCHAR(10),\
                AIR_TIME VARCHAR(10),\
                DISTANCE INTEGER,\
                WHEELS_ON VARCHAR(10),\
                TAXI_IN VARCHAR(10),\
                SCHEDULED_ARRIVAL VARCHAR(10),\
                ARRIVAL_TIME VARCHAR(10),\
                ARRIVAL_DELAY VARCHAR(10),\
                DIVERTED VARCHAR(10),\
                CANCELLED VARCHAR(10),\
                CANCELLATION_REASON VARCHAR(100) NULL,\
                AIR_SYSTEM_DELAY INTEGER DEFAULT NULL,\
                SECURITY_DELAY INTEGER DEFAULT NULL,\
                AIRLINE_DELAY INTEGER DEFAULT NULL,\
                LATE_AIRCRAFT_DELAY INTEGER DEFAULT NULL,\
                WEATHER_DELAY INTEGER DEFAULT NULL\
                )'
        inlist = '("YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE",\
                    "FLIGHT_NUMBER", "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCHEDULED_DEPARTURE", \
                    "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAXI_OUT", "WHEELS_OFF", "SCHEDULED_TIME", \
                    "ELAPSED_TIME", "AIR_TIME", "DISTANCE", "WHEELS_ON", \
                    "TAXI_IN", "SCHEDULED_ARRIVAL","ARRIVAL_TIME","ARRIVAL_DELAY","DIVERTED","CANCELLED","CANCELLATION_REASON","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?, ?)'
        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/flight.csv',train_percentage,valid_percentage,test_percentage,batch_size)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_adult_data(connection, schema=None, batch_size=10000, force=False,train_percentage=.50,valid_percentage=.40,test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "ADULT_DATA_FULL_TBL"
        training_tbl = "ADULT_DATA_TRAIN_TBL"
        validation_tbl = "ADULT_DATA_VALIDATION_TBL"
        test_tbl = "ADULT_DATA_TEST_TBL"
        tables = [full_tbl, training_tbl, validation_tbl, test_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                ID INTEGER generated by default as identity,\
                AGE INTEGER DEFAULT NULL,\
                WORKCLASS VARCHAR(50) NULL,\
                FNLWGT INTEGER NULL,\
                EDUCATION VARCHAR(100) NULL,\
                EDUCATIONNUM INTEGER,\
                MARITALSTATUS VARCHAR(50),\
                OCCUPATION VARCHAR(50),\
                RELATIONSHIP VARCHAR(50),\
                RACE VARCHAR(50),\
                SEX VARCHAR(10),\
                CAPITALGAIN INTEGER NULL,\
                CAPITALLOSS INTEGER NULL,\
                HOURSPERWEEK INTEGER NULL,\
                NATIVECOUNTRY VARCHAR(50),\
                INCOME VARCHAR(50)\
                )'
        inlist = '("AGE", "WORKCLASS", "FNLWGT", "EDUCATION", "EDUCATIONNUM",\
                    "MARITALSTATUS", "OCCUPATION", "RELATIONSHIP", "RACE", "SEX", \
                    "CAPITALGAIN", "CAPITALLOSS", "HOURSPERWEEK", "NATIVECOUNTRY","INCOME") \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/adult.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_data_auto(connection, table_descriptions, cols, inlist,filename,train_percentage=.50,valid_percentage=.40,test_percentage=.10, batch_size=10000,force=False):
        file_count = 0
        total_percentage = train_percentage + valid_percentage + test_percentage
        if total_percentage == 1:
            for line in open(filename): file_count += 1
            try:
                schema = table_descriptions[0].split('.')[0]
                tablename = table_descriptions[0].split('.')[1]
                full_set = connection.table(tablename)
                count = full_set.count()
                if count == file_count and force == False:
                    print("Table {} exists and data exists".format(tablename))
                    return
                else:
                    for i in range(len(table_descriptions)):DataSets._drop_and_create_table(connection,table_descriptions[i],cols)
                    DataSets.file_load(connection,table_descriptions,cols,inlist,filename,file_count,train_percentage,valid_percentage,test_percentage,batch_size)
            except:
                print("Table {} doesn't exist in schema {}".format(tablename,schema))
                print("Creating table {} in schema {} ....".format(tablename,schema))
                for i in range(len(table_descriptions)):DataSets._drop_and_create_table(connection,table_descriptions[i],cols)
                DataSets.file_load(connection,table_descriptions,cols,inlist,filename,file_count,train_percentage,valid_percentage,test_percentage,batch_size)
        else:
            print("Invalid Value Error: Sum of train_percentage({}), valid_percentage({}), test_percentage({}) not equal to 1".format(train_percentage,valid_percentage,test_percentage))
            return

    @staticmethod
    def insert_data(connection,tablename,cols,inlist,data, batch_size):
            sql = 'insert into ' + tablename + inlist
            if len(data) > 0:
                with connection.connection.cursor() as cur:
                    rows_inserted = cur.executemany(sql, data)
    @staticmethod
    def file_load(connection,table_descriptions,cols, inlist, filename,file_count,train_percentage,valid_percentage,test_percentage,batch_size):
        with open(filename, 'r') as my_file:
            reader = csv.reader(my_file, delimiter=',')
            data = list()
            data_list = list()
            load_count = 0
            for row in reader:
                remain_count = file_count - load_count
                if remain_count < batch_size:
                    batch_size = remain_count
                if len(data) <= batch_size:
                    data.append(list(row))
                    if len(data) == batch_size:
                        DataSets.split_data_into_tables(connection,data,table_descriptions,train_percentage,valid_percentage,test_percentage, cols, inlist, batch_size, file_count)
                        load_count += len(data)
                        data = list()
                        print("Data Loaded:{}%".format(math.floor(load_count/file_count*100)))                     
    @staticmethod
    def drop_table(connection,tablename,schema=None,):
        if schema is None:
            schema = Settings.user
        table = schema + '.' + tablename
        sql = "DROP TABLE " + tablename
        try:
            with connection.connection.cursor() as cur:
                cur.execute(sql)
                print("Drop Successful")
        except:
            print("Drop unsuccessful")
            pass

    @staticmethod
    def split_data_into_tables(connection, data,table_descriptions, train_percentage,valid_percentage,test_percentage, cols, inlist, batch_size, file_count):
        data_list = list()
        data = [[element or None for element in sublist] for sublist in data]
        random.seed(4)
        random.shuffle(data)
        data_list.append(data)
        full_data_count = len(data)
        train_rec = 0
        if train_percentage > 0:
            train_rec = math.floor(full_data_count * train_percentage)
            data_list.append(data[0:train_rec])
        else:
            data_list.append([])
        valid_rec = train_rec
        if valid_percentage > 0:
            valid_rec += math.floor((full_data_count * valid_percentage))
            data_list.append(data[train_rec:valid_rec])
        else:
            data_list.append([])
        test_rec = valid_rec
        if test_percentage > 0:
            test_rec += math.floor((full_data_count * test_percentage))
            data_list.append(data[valid_rec:full_data_count])
        else:
            data_list.append([])
        for i in range(len(table_descriptions)):DataSets.insert_data(connection,table_descriptions[i],cols,inlist,data_list[i],batch_size)

    @staticmethod
    def load_diabetes_data(connection,
                           schema=None,
                           batch_size=10000,
                           force=False,
                           train_percentage=.80,
                           valid_percentage=.10,
                           test_percentage=.10):
        if schema is None:
            schema = Settings.user
        full_tbl = "PIMA_INDIANS_DIABETES_TBL"
        training_valid_tbl = "PIMA_INDIANS_DIABETES_TRAIN_VALID_TBL"
        test_tbl = "PIMA_INDIANS_DIABETES_TEST_TBL"
        validation_tbl = "PIMA_INDIANS_DIABETES_VALIDATION_TBL"
        tables = [full_tbl, training_valid_tbl, test_tbl, validation_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = ('(ID INTEGER generated by default as identity,' +
            'PREGNANCIES INTEGER, '+
            'GLUCOSE INTEGER, '+
            'BLOODPRESSURE INTEGER, '+
            'SKINTHICKNESS INTEGER, '+
            'INSULIN INTEGER, '+
            'BMI DOUBLE, '+
            'PEDIGREE DOUBLE, '+
            'AGE INTEGER, '+
            'CLASS INTEGER)')
        inlist = ('("PREGNANCIES", "GLUCOSE", "BLOODPRESSURE", "SKINTHICKNESS", '+
                  '"INSULIN", "BMI", "PEDIGREE", "AGE", "CLASS") '+
                  'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
        DataSets.load_data_auto(connection, fq_tables, cols, inlist,
                                '../datasets/pima-indians-diabetes.csv',
                                train_percentage, valid_percentage,
                                test_percentage, batch_size, force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)

    @staticmethod
    def load_shampoo_data(connection, schema=None, batch_size=10000, force=False,train_percentage=1.0,valid_percentage=0.0,test_percentage=0.0):
        if schema is None:
            schema = Settings.user
        full_tbl = "SHAMPOO_SALES_DATA_TBL"
        tables = [full_tbl]
        fq_tables = [schema + '.' + tbl for tbl in tables]
        cols = '( \
                ID INTEGER NULL,\
                SALES DOUBLE NULL\
                )'
        inlist = '("ID", "SALES") \
                    VALUES (?, ?)'
        DataSets.load_data_auto(connection, fq_tables, cols, inlist,'../datasets/shampoo.csv',train_percentage,valid_percentage,test_percentage, batch_size,force)
        if len(tables) == 1:
            return tables[0]
        return tuple(tables)