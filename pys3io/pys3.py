
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
    This is a BOTO wrapper (https://github.com/boto/boto)  that includes the following methods:

    ListFolder(self,folder):
    StreamFile(self,file_path):
    DwnldFile(self,S3_file_path, local_file_path):
    CloseConnection(self):
    """

    def __init__(self, bucket_name, config_file = 'etc\config.ini'):
        """Instantiate the insance and open an active connection.
        """
        # Loading BOTO:
        import boto
        # Setting configs:
        configs = configparser.ConfigParser()
        configs.read(config_file)
        # Saving input var from the config file as a dictionary:
        s3_var = dict(configs.items('s3_var'))

        # Initiating variables:
        self.config_file = config_file
        self.bucket_name = bucket_name

        self.aws_key = s3_var['aws_access_key_id']
        self.aws_secret = s3_var['aws_secret_access_key']
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

    def ListFolder(self, folder):
        '''This method lists the files in a specified folder.
        '''

        for key in self.bucket.list(prefix='{}/'.format(folder), delimiter='/'):
            print(key.name)

    def StreamFile(self, file_path):
        '''This method streams a file (that is specified within the file_path) from S3 connection.
        '''

        file_stream = self.bucket.get_key('{}'.format(file_path))
        return file_stream

    def DwnldFile(self, S3_file_path, local_directory_path, file_name=False):
        '''This method downloads a specified S3_file_path to a specified local_file_path.
        '''

        file2dwnld = self.bucket.get_key('{}'.format(S3_file_path))

        keyString = str(file2dwnld.key).split('/')[-1]
        # check if file exists locally, if not: download it
        if not file_name:
            if not os.path.exists(local_directory_path + keyString):
                file2dwnld.get_contents_to_filename(local_directory_path + keyString)
            else:
                print 'file already exist in local path'
        else:
            if not os.path.exists(local_directory_path + keyString):
                file2dwnld.get_contents_to_filename(local_directory_path + file_name)
            else:
                print 'file already exist in local path'

    def CloseConnection(self):
        ''' This method terminates the S3 connection.
        '''
        self.connection.close()
        print 'Connection Terminated by User'
        self.status = 'Terminated'
