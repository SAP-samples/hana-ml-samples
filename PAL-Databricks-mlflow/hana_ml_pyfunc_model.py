import mlflow
from mlflow import pyfunc
from mlflow.models import set_model
import hana_ml
from hana_ml import dataframe
from hana_ml.model_storage import ModelStorage
import os

class CustomException(Exception):
    """Exception raised to get messages
    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message="HANA ML Pyfunc Custom Exception"):
        self.message = message
        super().__init__(self.message)

class hana_ml_pyfunc_model(pyfunc.PythonModel):

    def connectToHANA(self, context):
        try:
            url =  os.getenv('hana_url') 
            port = os.getenv('hana_port')
            user = os.getenv('hana_user')
            passwd = os.getenv('hana_password')
            connection_context = dataframe.ConnectionContext(url, port, user, passwd)
            return connection_context
        except Exception as e:
            print(f"Exception occurred: {e}")
            raise e
            return "Exception:{e}", e
    @mlflow.trace
    def load_context(self, context):
        try: 
            with mlflow.start_span("load_context"):
                self.model = context.artifacts["model"]
                self.connection_context = self.connectToHANA(context)
                print("HANA_ML_MODEL loaded in load_context")
        except Exception as e:
            print(f"Exception occurred: {e}")
            raise Exception(f"Loading the context failed due to {e}")
    @mlflow.trace
    def predict(self, context, model_input):
        table_name = None
        try: 
            if self.connection_context.connection.isconnected() == False:
                with mlflow.start_span("connect_to_HANA"):
                    self.connection_context = self.connectToHANA(context)
                    if self.connection_context.connection.isconnected():
                        print("HANA Connection Successful")
                    else:
                        raise Exception("HANA Connection Failed")
       
            with mlflow.start_span("load_model"):
                hana_model = ModelStorage.load_mlflow_model(connection_context=self.connection_context, model_uri=self.model,use_temporary_table=False, force=True)
           
                print("HANA_ML_MODEL loaded in predict")
                print("model_input", model_input)
                table_name = str(model_input["INFERENCE_TABLE_NAME"][0]) 
                print("Table Name:", table_name)
            with mlflow.start_span("hana_ml_predict"):
                df = self.connection_context.table(table_name)
                if df.count() > 0:
                  
                    print(f"Running HANA ML inference on {table_name} with {df.count()} records")
                    prediction = hana_model.predict(df, key = "ID").collect()
                    print("Prediction completed")
                    return  prediction
                else:
                    raise Exception(f"HANA Inference Table {table_name} is empty")
        
            
        except Exception as e:
        
            print(f"Exception occurred: {e}")
            raise f"Exception:{e}"
        
set_model(hana_ml_pyfunc_model())