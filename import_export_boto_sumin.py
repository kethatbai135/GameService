import boto3
import sys
import os
from boto3.dynamodb.conditions import Key

#os.environ['HTTP_PROXY'] = 'http://168.219.61.252:8080'
#os.environ['HTTPS_PROXY'] = 'https://168.219.61.252:8080'
   
#dev
access_key = ''
secret_key = ''
aws_region =['us-east-1']

connection = []

get_pkg_name = []
get_mcc = []
get_cafe_id = []
get_is_google_play = []
get_is_one_store = []
get_reg_date = []

def connect_with_boto3():
 try:
  area_num = len(aws_region)
  for i in range(0,area_num):
   connection.append(boto3.client('dynamodb',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name=aws_region[i],use_ssl=False))
  print "boto3 conn complete"
 except:
  print "error boto3 connection"
 return connection
   
def scan_item(dyconn):
 print "scan_item"
 area_num = len(aws_region)
 for i in range(0,area_num):
  response = dyconn[i].scan(TableName='TB_TOOLS_NAVER_CAFE_sumin')
   
 #Select = ALL_ATTRIBUTES
 f_write = open("backup_data.csv",'w')
 for row in response['Items']:
  f_write.write(str(row['pkg_name']['S'])+"|"+str(row['mcc']['S'])+"|"+str(row['cafe_id']['S'])+"|"+str(row['is_google_play']['S'])+"|"+str(row['is_one_store']['S'])+"|"+str(row['reg_date']['S'])+"\n")

def get_txtfile(lastNumber,File_Name):
    print "get_txtfile"
    f_read = open(File_Name,'r')
    for i in range(0,lastNumber):
        lines = f_read.readline()
        lines = lines[:-1]
        splitdata = lines.split('|')
        get_pkg_name.append(splitdata[0])
        get_mcc.append(splitdata[1])
        get_cafe_id.append(splitdata[2])
        get_is_google_play.append(splitdata[3])
        get_is_one_store.append(splitdata[4])
        get_reg_date.append(splitdata[5])
    f_read.close()

def create_table(dyconn):
 print "create_table"
 area_num = len(aws_region)
 for i in range(0,area_num):
  table = dyconn[i].create_table(TableName='TB_TOOLS_NAVER_CAFE_sumin_boto'
            ,KeySchema=[{'AttributeName':'pkg_name','KeyType':'HASH'},{'AttributeName':'mcc','KeyType':'RANGE'}]
            ,AttributeDefinitions=[{'AttributeName':'pkg_name','AttributeType':'S'},{'AttributeName':'mcc','AttributeType':'S'}
                   ,{'AttributeName':'reg_date','AttributeType':'S'}]
            ,GlobalSecondaryIndexes=[{'IndexName':'pkg_name-reg_date-index'
                     ,'KeySchema':[{'AttributeName':'pkg_name','KeyType':'HASH',},{'AttributeName':'reg_date','KeyType':'RANGE'}]
                     ,'Projection':{'ProjectionType':'ALL'}
                     ,'ProvisionedThroughput':{'ReadCapacityUnits': 10,'WriteCapacityUnits': 10}}]
            ,ProvisionedThroughput={'ReadCapacityUnits':10,'WriteCapacityUnits':100}
            )
  print table
 
def add_dynamodb_tag(dnconn,m_tablename,m_value):
    print "add_dynamodb_tag"
    area_num = len(aws_region)
    for i in range(0,area_num):
		ArnName="arn:aws:dynamodb:"+aws_region[i]+":"+account+":table/"+m_tablename
		response = dnconn[i].tag_resource(ResourceArn=ArnName,Tags=[{'Key':'Environment1','Value':m_value}])
 

def put_item(dyconn):
 print "put_item"
 area_num = len(aws_region)
 item_num = len(get_pkg_name)
 for i in range(0,area_num):
  for j in range(0,item_num):
     response = dyconn[i].put_item(TableName='TB_TOOLS_NAVER_CAFE_sumin_boto'
         ,Item={'pkg_name':{'S':get_pkg_name[j]}
                ,'mcc':{'S':get_mcc[j]}
                ,'cafe_id':{'S':get_cafe_id[j]}
                ,'is_google_paly':{'S':get_is_google_play[j]}
                ,'is_one_store':{'S':get_is_one_store[j]}
                ,'reg_date':{'S':get_reg_date[j]} })       #'key_name':{'keytype':'keyvalue'}
  print response
     
def main(argv):
 conn = connect_with_boto3()
 #scan_item(conn)
 #create_table(conn)
 get_txtfile(1324,'backup_data.csv')
 put_item(conn)
   
if __name__ == "__main__":
    main(sys.argv)