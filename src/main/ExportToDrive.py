from __future__ import (unicode_literals, absolute_import, print_function,division)
import sys
sys.path.append("/home/anthonyrathe/repos/BRIKSScreener")
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from os.path import dirname as dirname
from os import chdir, listdir, stat
import os


def authenticate():
	"""
		Authenticate to Google API
	"""

	gauth = GoogleAuth(settings_file="{}/credentials/settings.yaml".format(dirname(dirname(dirname(__file__)))))

	return GoogleDrive(gauth)



def upload_files(drive, folder_id, src_folder_name):
	"""
		Upload files in the local folder to Google Drive
	"""

	# Enter the source folder
	try:
		chdir(src_folder_name)
	# Print error if source folder doesn't exist
	except OSError:
		print(src_folder_name + 'is missing')
	# Auto-iterate through all files in the folder.
	for file1 in listdir('.'):
		# Check the file's size
		statinfo = stat(file1)
		if statinfo.st_size > 0:
			print('uploading ' + file1)
			# Upload file to folder.
			f = drive.CreateFile(
				{"parents": [{"kind": "drive#fileLink", "id": folder_id}]})
			f.SetContentFile(file1)
			f.Upload()
		# Skip the file if it's empty
		else:
			print('file {0} is empty'.format(file1))

def empty_folder(drive, folder_id):
	file_list = drive.ListFile({'q': "'{}' in parents and trashed=false".format(folder_id)}).GetList()
	for file in file_list:
		file.Delete()





base_path = "{}/data/cleaned/overview_ppt/".format(dirname(dirname(dirname(__file__))))
src_folder_name = os.path.relpath(base_path)

# Authenticate to Google API
drive = authenticate()
folder_id = '1ygXmnkJEkOn4XXqgP1sf6VHy3K-HKAOT'

# Empty the folder
empty_folder(drive,folder_id)

# Upload the files
upload_files(drive, folder_id, src_folder_name)

