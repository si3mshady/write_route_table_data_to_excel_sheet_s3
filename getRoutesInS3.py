from os import stat
import boto3, glob
from threading import Thread
import pandas as pd


REGION = 'us-east-1'
BUCKET_NAME = 'elliotts-arnolds-weekend-python-cohort'
ec2 = boto3.client('ec2', region_name=REGION)
FILE_NAME = 'output.xlsx'
GREP_FOR = "vpc*"


def get_all_vpc_ids() -> list:
    #list comprehension within anon func
    return  [ v.get('VpcId') for v in  (lambda i: i.get('Vpcs'))(ec2.describe_vpcs())]

def filter_route_table_by_vpc(vpc: list):

    for v in vpc:
        destinationCidrBlock, gateway_id, origin, state = [],[],[],[]
        kwargs = {"Filters":[{"Name":"vpc-id", "Values": [v]}]}
        rt_dict = {rt.get('RouteTableId'):rt.get('Routes') for rt in  ec2.describe_route_tables(**kwargs).get('RouteTables')}
        keys = list(rt_dict.keys())

        #format json for use with pandas dataframe object 
        for key in keys: #route table id 
            for index,_ in enumerate(rt_dict[key]): #iterate over list 
                destinationCidrBlock.append(rt_dict[key][index].get('DestinationCidrBlock'))
                gateway_id.append(rt_dict[key][index].get('GatewayId'))
                origin.append(rt_dict[key][index].get('Origin'))
                state.append(rt_dict[key][index].get('State'))
        #column names will be the keys in spreadsheet 
        obj = {"gateway_id":gateway_id,"dest_cidr":destinationCidrBlock, "rt":key, "origin": origin,"state":state }
        write_to_excel(obj, v, key)

def write_to_excel(obj, v,r):
    res = pd.DataFrame(obj)
    res.to_excel(f"{v}-{FILE_NAME}", f'{r}.xlsx')

def handle_files():
    files = glob.glob(GREP_FOR)

    return files
  
def send_to_s3(filename,bucket):

    s3 = boto3.client('s3', region_name=REGION)
    s3.upload_file(filename, BUCKET_NAME, filename)
    print(f'Uploaded file {filename} to s3')


def init():
    vpc_list = get_all_vpc_ids()
    filter_route_table_by_vpc(vpc_list)


if __name__ == "__main__":
    init()
    file_names = handle_files()
    TREADS = []
    for index,filename in enumerate(file_names):
        t = Thread(target=send_to_s3, args=(filename,BUCKET_NAME))
        TREADS.append(t)
        t.start()

    # wait for the threads to complete
    for t in TREADS:
        t.join()

#Elliott Arnold - threading practice & pandas practice 
#Fetch route table data from each vpc, format and write to excel sheet
#5-21-22  7-11 




