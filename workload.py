from boto3 import client as boto3_client
import os

inputBucket = "inputbucket117"
outputBucket = "outputbucket117"
testCases = "test_cases/"

def clear_inputBucket():
	global inputBucket
	s3 = boto3_client('s3')
	list_obj = s3.list_objects_v2(Bucket=inputBucket)
	try:
		for item in list_obj["Contents"]:
			key = item["Key"]
			s3.delete_object(Bucket=inputBucket, Key=key)
	except:
		print("Nothing to clear in input bucket")
	
def clear_outputBucket():
	global outputBucket
	s3 = boto3_client('s3')
	list_obj = s3.list_objects_v2(Bucket=outputBucket)
	try:
		for item in list_obj["Contents"]:
			key = item["Key"]
			s3.delete_object(Bucket=outputBucket, Key=key)
	except:
		print("Nothing to clear in output bucket")

def upload_to_inputBuckets3(path, name):
	global inputBucket
	s3 = boto3_client('s3')
	s3.upload_file(path + name, inputBucket, name)
	
	
def upload_files(test_case):	
	global inputBucket
	global outputBucket
	global testCases
	
	
	test_dir = testCases + test_case + "/"
	

	for filename in os.listdir(test_dir):
		if filename.endswith(".mp4") or filename.endswith(".MP4"):
			print("Uploading to input bucket..  name: " + str(filename)) 
			upload_to_inputBuckets3(test_dir, filename)
			
	
def workload_generator():
	
	print("Executing Test Case 1")
	upload_files("test_case_1")

	print("Executing Test Case 2")
	upload_files("test_case_2")
	

# clear_inputBucket()
clear_outputBucket()	
workload_generator()	

	

