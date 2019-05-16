#!/usr/bin/env python3
##########################################################################
### Athena Unit Testing tool
### Author: Pawel Walat
### Current Version: 1.0
###
### Prerequisites:  1) please run: pip install PTable
###                 2) setup account in awscli
###
### Change Log:
###     0.1: 22-Dec-2018 - Base version (Pawel Walat)
##########################################################################
from prettytable import PrettyTable
import boto3
import datetime
import xml.etree.ElementTree
import xml.etree
import sys

# Function for starting athena query
def run_query(query, database, s3_output):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            },
        ResultConfiguration={
            'OutputLocation': s3_output,
            }
        )
    return response['QueryExecutionId']

# Function  to print summary table
def quick_summary():
    i=0
    passed=0
    failed=0
    print ("Summary:")
    x = PrettyTable()
    x.field_names = ["No.", "Name", "Actual", "Operator", "Expected", "Execution status","Test Status"]
    for testcase in root:
        i=i+1
        test_status = testcase.find('test_status').text
        if test_status == 'FAILED':
            failed=failed+1
            test_status = '!!!FAILED!!!' ## highlight failed test cases
        if test_status == 'PASSED':
            passed = passed + 1
        if testcase.find('operator') == None:
            operator="="
        else:
            operator=testcase.find('operator').text
        x.add_row([i,
                   testcase.attrib['name'],
                   testcase.find('result').text,
                   operator,
                   testcase.find('expected').text,
                   testcase.find('execution_status').text,
                   test_status]
                   )
    print(x)
    total=i
    x2 = PrettyTable()
    x2.field_names = ["Status", "Count"]
    x2.add_row(["TOTAL",total])
    x2.add_row(["PASSED", passed ])
    x2.add_row(["FAILED", failed])
    if (failed+passed) != 0 :
        x2.add_row(["PASSED %", str(round(passed/(total)*100,0))+" %"])
    print(x2)
    table_txt = x.get_string()
    table_txt2 = x2.get_string()
    with open(resultfile+'.txt', 'w') as file:
        file.write(table_txt)
        file.write(table_txt2)

# Handle command prompt
if len(sys.argv) != 3 and len(sys.argv) != 2:
    sys.exit("Usage: "+sys.argv[0]+" [test_xml_file] <[environment_suffx>")

if sys.argv[1][-4:].upper() == ".XML": # Handle filenames with and without .xml extension
    file = sys.argv[1][:-4]
else:
    file=sys.argv[1]

if len(sys.argv) == 3:
    env = sys.argv[2]
else:
    env = ""

client = boto3.client('athena')

et = xml.etree.ElementTree.parse(file+".xml")
root = et.getroot()

currentDT = datetime.datetime.now()
root.attrib['Execution date'] = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"
root.attrib['db'] = root.attrib['db']+env
resultfile=file+env+"_"+str(f"{datetime.datetime.now():%Y%m%d %H%M%S}")
log=resultfile+".xml"

#Send all SQLs to Athena for execution
for testcase in root:
    print ('-------------------------------------------')
    print('Parsing testcase: '+str(testcase.attrib['name']))
    try:
        query_id = run_query(testcase.find('sql').text, root.attrib['db'],root.attrib['s3_output'])
        testcase.find('execution_status').text = 'SENT TO ATHENA'
        testcase.find('queryexecutionid').text = query_id
        testcase.find('execution_start').text =f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"
    except:
        testcase.find('test_status').text = 'FAILED'
        testcase.find('execution_status').text = 'PARSING ERROR'
et.write(log)

#Check for results in the loop
j=0
all_completed=False
while(not all_completed):
    all_completed=True
    for testcase in root:
        j = j + 5
        if j % 1 == 0:
            quick_summary()
        print ('-------------------------------------------')
        print('Checking for results of testcase: '+str(testcase.attrib['name']))
        #print(testcase.find('sql').text)
        if testcase.find('execution_status').text in {"SUCCEEDED", "FAILED","PARSING ERROR"}:
            print("continue")
            continue
        testcase.find('execution_status').text=client.get_query_execution(QueryExecutionId=testcase.find('queryexecutionid').text)['QueryExecution']['Status']['State']
        if testcase.find('operator') == None:
            operator="="
        else:
            operator=testcase.find('operator').text
        if testcase.find('execution_status').text =="SUCCEEDED":
            response = client.get_query_results(
            QueryExecutionId=testcase.find('queryexecutionid').text,MaxResults=2) #get only 1 row from resultset
            testcase.find('result').text = response['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
            testcase.find('execution_completed').text = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"
            #Handling different operators
            # =
            if operator == "=" or operator == "==":
                if testcase.find('expected').text == testcase.find('result').text:
                    testcase.find('test_status').text = 'PASSED'
                else:
                    testcase.find('test_status').text = 'FAILED'
            elif operator == ">":
                if testcase.find('expected').text > testcase.find('result').text:
                    testcase.find('test_status').text = 'PASSED'
                else:
                    testcase.find('test_status').text = 'FAILED'
            elif  operator == ">=" or operator == "=>":
                if testcase.find('expected').text >= testcase.find('result').text:
                    testcase.find('test_status').text = 'PASSED'
                else:
                    testcase.find('test_status').text = 'FAILED'
            elif  operator == "<":
                if testcase.find('expected').text < testcase.find('result').text:
                    testcase.find('test_status').text = 'PASSED'
                else:
                    testcase.find('test_status').text = 'FAILED'
            elif  operator == "<=" or operator == "=<":
                if testcase.find('expected').text <= testcase.find('result').text:
                    testcase.find('test_status').text = 'PASSED'
                else:
                    testcase.find('test_status').text = 'FAILED'
            elif operator == "<>" or operator == "!=":
                if testcase.find('expected').text != testcase.find('result').text:
                    testcase.find('test_status').text = 'PASSED'
                else:
                    testcase.find('test_status').text = 'FAILED'
            else:
                    testcase.find('test_status').text = 'FAILED'

        elif testcase.find('execution_status').text =='FAILED': #Test result is FAILED when SQL execution fails
                testcase.find('test_status').text = 'FAILED'
        else:
            all_completed = False
    et.write(log)
    j=j+5
    if j%1==0:
        quick_summary()

#Print summary at the end
print("All test cases completed!")
quick_summary()
