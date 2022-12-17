import os

os.environ["env"] = "TEST"
env = os.environ["env"]

pwd = os.getcwd()
dim_city_file_path = pwd + "\\..\\staging\\dimension_city"
fact_file_path = pwd + "\\..\\staging\\fact"

