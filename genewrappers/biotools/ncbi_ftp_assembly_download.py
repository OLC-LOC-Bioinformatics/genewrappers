#!/usr/bin/env python3
from olctools.accessoryFunctions.accessoryFunctions import SetupLogging
from argparse import ArgumentParser
from click import progressbar
from threading import Thread
from subprocess import call
from queue import Queue
from time import sleep
import logging
import csv
import os

__author__ = 'adamkoziol'


class AssemblyDownload(object):

    def main(self):
        self.parse_csv()
        if self.sleeptime:
            self.sleep()
        self.download_assembly()

    def parse_csv(self):
        """
        
        """
        logging.info('Loading metadata table')
        # Load the file into a dictionary using the csv library
        csv_dict = csv.DictReader(open(self.metadatatable), delimiter=',')
        for i, row in enumerate(csv_dict):
            try:
                # Only populate the dictionary if the isolate and assembly fields have been included
                if row['Isolate'] and row['Assembly']:
                    self.assembly_dict[i] = {
                        row['Isolate']: row['Assembly']
                    }
            except KeyError:
                print('Something went wrong parsing {mt} please ensure that the "Isolate" and "Assembly" headers are '
                      'present in the file'.format(mt=self.metadatatable))

    def sleep(self):
        """
        If desired, allow the program for a desired amount of time. Include a fun progress bar!
        """
        logging.info('Sleeping for {st} seconds'.format(st=self.sleeptime))
        # Determine how many steps the sleep time should be divided into
        sleep_step = int(self.sleeptime / 10)
        # Create a list of the total sleep range in seconds, using the total time as the stop step, and the number of
        # steps
        sleep_range = [i for i in range(0, self.sleeptime, sleep_step)]
        # Run the progress bar over the total range of seconds to sleep
        with progressbar(sleep_range, show_eta=False) as bar:
            for _ in bar:
                # Sleep for the appropriate amount of time
                sleep(sleep_step)

    def download_assembly(self):
        """
        Run the multi-threaded download method
        """
        for i in range(self.threads):
            # Send the threads to
            threads = Thread(target=self.threaded_download, args=())
            # Set the daemon to true - something to do with thread management
            threads.setDaemon(True)
            # Start the threading
            threads.start()
        # Iterate through all the entries with isolate and assembly entries
        for i in self.assembly_dict:
            for isolate, assembly in self.assembly_dict[i].items():
                # ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/001/696/305/GCF_001696305.1_UCN72.1/
                # GCF_001696305.1_UCN72.1_genomic.gbff.gz .
                self.queue.put((isolate, assembly))
                # self.queue.put((isolate, assembly))
        self.queue.join()

    def threaded_download(self):
        while True:
            # Extract the isolate and assembly names from the queue
            isolate, assembly = self.queue.get()
            # Slice the assembly in order to get the pieces necessary to get the FTP URL
            assembly_first_three = assembly[0:3]
            assembly_second_three = assembly[4:7]
            assembly_third_three = assembly[7:10]
            assembly_fourth_three = assembly[10:13]
            # We are only interested in the assemblies
            compressed_assembly_remote = '{assembly}_{isolate}_genomic.fna.gz' \
                .format(assembly=assembly, isolate=isolate)
            # Set the local names and paths of the assembly as it is downloaded, decompressed, and finally moved to the
            # desired final naming scheme
            compressed_assembly = os.path.join(self.path, compressed_assembly_remote)
            decompressed_assembly = os.path.join(self.path, '{assembly}_{isolate}_genomic.fna'
                                                 .format(assembly=assembly, isolate=isolate))
            final_assembly = os.path.join(self.path, '{assembly}_{isolate}.fasta'
                                          .format(assembly=assembly, isolate=isolate))
            # Only download the file is none of the potential downstream files exist
            if not os.path.isfile(compressed_assembly) and not os.path.isfile(
                    decompressed_assembly) and not os.path.isfile(final_assembly):
                # Use wget to access the FTP server. Use the strict naming scheme to find the correct URL. -P sets the
                # output path. Chain the decompressing and renaming steps (with gunzip, and mv, respectively) to the
                # command
                wget_cmd = 'wget ftp://ftp.ncbi.nlm.nih.gov/genomes/all/{first}/{second}/{third}/' \
                           '{fourth}/{assembly}_{isolate}/{car} -P {path} && gunzip {ca} && mv {da} {fa}' \
                    .format(first=assembly_first_three,
                            second=assembly_second_three,
                            third=assembly_third_three,
                            fourth=assembly_fourth_three,
                            assembly=assembly,
                            isolate=isolate,
                            car=compressed_assembly_remote,
                            path=self.path,
                            ca=compressed_assembly,
                            da=decompressed_assembly,
                            fa=final_assembly)
                call(wget_cmd, shell=True)
            # Run system calls that attempt to run the second (decompression) and third (renaming) parts of the call
            # in case it was interrupted
            if os.path.isfile(compressed_assembly) and not os.path.isfile(decompressed_assembly):
                gunzip_cmd = 'gunzip {ca} && mv {da} {fa}'.format(ca=compressed_assembly,
                                                                  da=decompressed_assembly,
                                                                  fa=final_assembly)
                call(gunzip_cmd, shell=True)
            if not os.path.isfile(final_assembly) and os.path.isfile(decompressed_assembly):
                mv_cmd = 'mv {da} {fa}'.format(da=decompressed_assembly,
                                               fa=final_assembly)
                call(mv_cmd, shell=True)
            self.queue.task_done()

    def __init__(self, path, accessiontable, threads, sleep):
        if path.startswith('~'):
            self.path = os.path.abspath(os.path.expanduser(os.path.join(path)))
        else:
            self.path = os.path.abspath(os.path.join(path))
        self.metadatatable = os.path.join(self.path, accessiontable)
        assert os.path.isfile(self.metadatatable), 'Cannot find supplied pathogen metadata table {at} in ' \
                                                   'supplied path {sp}' \
            .format(at=self.metadatatable,
                    sp=self.path)
        self.threads = threads
        self.sleeptime = sleep
        if self.sleeptime:
            assert self.sleeptime > 10, 'Must sleep at least 10 seconds'
            assert self.sleeptime < 86400, 'Cannot sleep for more than 24 hours'
        self.assembly_dict = dict()
        self.queue = Queue(maxsize=self.threads)
        logging.info('Starting pathogen assembly download using {at}'.format(at=self.metadatatable))


def cli():
    # Parser for arguments
    parser = ArgumentParser(description='Downloads and decompresses FASTA assemblies from the NCBI FTP')
    parser.add_argument('-p', '--path',
                        required=True,
                        help='Path to folder containing necessary tables')
    parser.add_argument('-a', '--accessiontable',
                        default='pathogens.csv',
                        help='Name of metadata table from NCBI (must be in the supplied path). Generate the table '
                             'from NCBI pathogens '
                             'e.g. https://www.ncbi.nlm.nih.gov/pathogens/isolates/#/search/taxgroup_name:%22Salmonella'
                             '%20enterica%22 '
                             'Select Download: -> Data type: Metadata -> Download. Default name is pathogens.csv')
    parser.add_argument('-n', '--numthreads',
                        default=3,
                        type=int,
                        choices=[1, 2, 3, 4, 5, 6],
                        help='Number of concurrent downloads to perform. Default is 3')
    parser.add_argument('-s', '--sleeptime',
                        default=0,
                        type=int,
                        help='Amount of time in seconds you would like the script to sleep until it starts the '
                             'download. Default is 0. NOTE: There are 3600 seconds in an hour.')
    arguments = parser.parse_args()
    SetupLogging()
    download = AssemblyDownload(path=arguments.path,
                                accessiontable=arguments.accessiontable,
                                threads=arguments.numthreads,
                                sleep=arguments.sleeptime)
    download.main()
    logging.info('NCBI assembly download complete!')


if __name__ == '__main__':
    cli()
