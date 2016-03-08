
# -*- coding: utf-8 -*-
"""
#### pys3io  v1.01 Beta #####
Created on Monday, February  01  10:02:04 2016
@author: Datuman a.k.a. kod5kod
https://github.com/kod5kod

Description:
A very helpful python -- S3 (AWS) input output that includes reading and streaming files line by line

Required config dictionary

"""

class PyS3:
    """
    A very helpful python -- S3 (AWS) input output that includes reading and streaming files line by line


    ListFolder(self,folder):
    StreamFile(self,file_path):
    DwnldFile(self,S3_file_path, local_file_path):
    CloseConnection(self):

    #Example of S3 setting configs:
    s3_conf_dict = {
                        'aws_access_key_id' : 'your_access_key',
                        'aws_secret_access_key' : 'your_secret_key'
                        }

    """
    import os


    def __init__(self, s3_conf_dict,bucket_name):
        """Instantiate the insance and open an active connection.
        """
        # Loading BOTO:
        import boto
        import os

        # Initiating variables:
        self.bucket_name = bucket_name
        self.aws_key = s3_conf_dict['aws_access_key_id']
        self.aws_secret = s3_conf_dict['aws_secret_access_key']

        # opening a connection:
        self.connection = boto.connect_s3(self.aws_key, self.aws_secret)
        self.bucket = self.connection.get_bucket(self.bucket_name)
        self.status = 'Active'


    def __str__(self):
        ''' Initiating a str method that gives meta information on the instance:
        '''
        sent1 = str(self.connection)
        sent2 = 'Connection\'s bucket is: ' + str(self.bucket_name)
        sent3 = 'AWS key is :' + self.aws_key
        sent4 = 'AWS Secret is :' + self.aws_secret
        sent5 = 'Connection is :' + self.status
        return '\n' + sent1 + '\n' + sent2 + '\n' + sent3 + '\n' + sent4 + '\n' + sent5


    def list_folder(self, folder):
        '''This method lists the files in a specified folder.
        '''
        for key in self.bucket.list(prefix='{}/'.format(folder), delimiter='/'):
            print(key.name)


    def stream_flile(self, file_path):
        '''This method streams a file (that is specified within the file_path) from S3 connection.
        '''
        file_stream = self.bucket.get_key('{}'.format(file_path))
        return file_stream


    def read_lines(self, file_path, function_on_line):
        '''This method streams and reads a file line by line.
        '''
        pipe = '' # initiating an empty queue
        file_stream = self.bucket.get_key('{}'.format(file_path)) # getting key
        file_stream.open() # opening a connection
        for byte in file_stream: # itterating over bytes
            pipe += byte
            lines = pipe.split('\n')
            temp = lines[-1]
            pipe = lines.pop(-1)
            for line in lines:
                function_on_line(line)
            pipe = temp


    def s3_2local(self, s3_file_path, local_directory_path, file_name=None):
        '''This method downloads a specified S3_file_path to a specified local_file_path.
        '''
        import os
        file2dwnld = self.bucket.get_key('{}'.format(s3_file_path))
        keyString = str(file2dwnld.key).split('/')[-1]
        # check if file exists locally, if not: download it
        if not file_name: # if file name is not specified (None):
            if not os.path.exists(os.path.join(local_directory_path , keyString)):
                file2dwnld.get_contents_to_filename(os.path.join(local_directory_path , keyString))
                print 'File has been copied.'
            else:
                print 'file already exist in local path'
        else: # if file name is  specified :
            if not  os.path.exists(os.path.join(local_directory_path , file_name)):
                file2dwnld.get_contents_to_filename(os.path.join(local_directory_path , file_name))
            else:
                print 'file already exist in local path'


    def local_2s3(self, local_file_path, S3_file_path):
        '''This method uploads a specified local file to a specified  S3 path.
        local_file_path = local file path to be uploaded
        S3_file_path = S3 file path to copy the local file into
        '''
        import sys
        def percent_cb(complete, total):
            sys.stdout.write('.')
            sys.stdout.flush()

        k = Key(self.bucket)
        k.key = '{}'.format(S3_file_path)
        k.set_contents_from_filename(local_file_path, cb=percent_cb, num_cb=10)
        print 'The file: \n\'{}\' was uploaded to S3 at the following path: \n\'{}\''.fomrat(local_file_path,S3_file_path)


    def close_connection(self):
        ''' This method terminates the S3 connection.
        '''
        self.connection.close()
        print 'Connection Terminated by User'
        self.status = 'Terminated'
