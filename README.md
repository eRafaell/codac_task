### Programming Exercise using PySpark

#### Background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

#### What is a purpose of the app?:
- reading csv file
- dropping unneeded columns
- renaming columns
- joining 2 DataFrames
- filtering rows with specific countries
- saving output file

#### Installation
Clone repository:
```bash
https://github.com/eRafaell/codac_task.git
```


Virtual environment installation:
```bash
pip install virtualenv
```


Virtual environment creation:
```bash
python -m venv my_env
```


Virtual environment activation:
```bash
source my_env/bin/activate
```


Required modules installation:
```bash
pip install -r requirements.txt
```


Run application with 3 required arguments:
- path1 = path to data set 1 
- path2 = path to data set 2 
- countries = list of countries to be filter out from data set

example:
```bash
python src/main.py -path1 "./input_datasets/dataset_one.csv" -path2 "./input_datasets/dataset_two.csv" -countries "United Kingdom" Netherlands
```