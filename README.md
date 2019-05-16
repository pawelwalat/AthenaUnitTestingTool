# AthenaUnitTestingTool
Tool to execute SQL unit tests on AWS athena databases

1. What is this tool for?
Athena Unit Testing tool is a Python script to execute simple SQL checks (unit tests) on AWS Athena Database.

2. How to use it?
First of all you need to prepare XML file with test plan. Please check example_testfile.xml to see how to do that.

Define Athena DB and S3 folder to store temporary SQL output:
	<tests db="testdb" s3_output="s3://mybucket/tmp">

Define test case with attribute name:
	<test name="This is test scenario number 1">

and with following elements:
		<sql>select 1</sql>		 - this is a SQL statement which will be executed
		<expected>1</expected>   - expected result (only first row will be checked)
		<operator>=</operator>	 - operator to compate expected and actual result. Pollisble values: > < != = >= <= (this parameter can be skipped, then operator = will be used).

Rest of elements should remain empty. Script use them to store test results:
		<result></result>		
		<execution_status></execution_status>
		<execution_start></execution_start>
		<execution_completed></execution_completed>
		<test_status></test_status>
		<queryexecutionid></queryexecutionid>

Execute test plan with:
python AthenaUTtool.py [test_xml_file] <[environment_suffx>
Example:
	python AthenaUTtool.py example_testfile.xml

If you want to specify environment_suffix, all tests will be executed on DB defined in XML with added suffix
Example:
	python AthenaUTtool.py example_testfile.xml DEV2
	(Queries will be executed on database: testdb-DEV2)

As an output you will get:

A. text output on the text console\
B. XML file with filled empty tags for each test case
C. TXT file with summary

2. Changelog

3. Todo
