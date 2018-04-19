import sys
import os
import argparse
import tarfile
import logging
import shutil

class UnpackTar(object):
    ''' Class for unpacking the tar-file with the
        data file to be analyzed.
    '''

    def __init__(self):

        # Create logger
        self.log = logging.getLogger(__name__)
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_formatter)
        self.log.addHandler(console_handler)

        # File for storing the extracted data files
        # ToDo: Include in configuration file when it is needed in various applications
        current_dir = os.path.dirname(os.path.realpath(__file__))
        self.output_path = os.path.join(current_dir, "extract_dir")


    def main(self, args):
        cmd_parser = self.parse_arguments(args)

        if cmd_parser.clean:
            self.clean_old_data_files()
        elif cmd_parser.tar_location:
            self.extract_tar(cmd_parser)
        else:
            print "Please run application with option."
            sys.exit(1)

    def parse_arguments(self, args):
        ''' Read command line options '''

        cmd_parser = argparse.ArgumentParser(description='New Yorker Application')
        cmd_parser.add_argument(
                        "--tar-location",
                        type=str,
                        help="Specify location to tar file") 
        cmd_parser.add_argument(
                        "--clean",
                        action="store_true",
                        help="Clean old Datafiles.")

        return cmd_parser.parse_args(args)

    def extract_tar(self, cmd_parser):
        ''' Function for the extraction of the tar-file'''

        tar_path = cmd_parser.tar_location

        if not os.path.exists(tar_path):
            self.log.error("Tar file does not exists: '"+ tar_path +"'")
            sys.exit(1)

        if os.path.exists(self.output_path):
            print "Data already exists in the output path: "+ self.output_path
            #ToDo: Enable check for deleting old data
            self.clean_old_data_files
        else:
            print "Extracting the data in "+ self.output_path
            os.makedirs(self.output_path)

            tar = tarfile.open(tar_path)
            tar.extractall(self.output_path)

            # Following just for information. Not used for productions code
            print "Extracted the following files: "
            for names in tar.getnames():
                print names

    def clean_old_data_files(self):
        ''' Remove existing files'''
        if not os.path.exists(self.output_path):
            self.log.error("No old data exists in: '"+ self.output_path +"'")
            sys.exit(1)
        else:
            self.log.info("Data will be deleted from: '"+ self.output_path +"'")
            shutil.rmtree(self.output_path)

if __name__ == '__main__':
    unpack_tar = UnpackTar()
    unpack_tar.main(sys.argv[1:])
