#!/usr/bin/env python3
from olctools.accessoryFunctions.accessoryFunctions import SetupLogging
from argparse import ArgumentParser
from subprocess import call
import multiprocessing
import logging
import shutil
import csv
import os


class SRAdownload(object):

    def main(self):
        self.parse_csv()
        self.download_fastq()

    def parse_csv(self):
        """
        Parse the supplied run info .csv file to extract the SRA accession as well as the desired sample naming column
        """
        logging.info('Loading Run Info table')
        # Load the file into a dictionary using the csv library
        csv_dict = csv.DictReader(open(self.runinfotable), delimiter=',')
        # Populate the dictionary with SRA accession: desired sample name
        for i, row in enumerate(csv_dict):
            try:
                self.name_dict[row['Run']] = row[self.column_name]
            except KeyError:
                print('Something went wrong parsing {rit} please ensure that the "Run" and "{cn}" headers are '
                      'present in the file'.format(rit=self.runinfotable,
                                                   cn=self.column_name))

    def download_fastq(self):
        logging.info('Downloading files from SRA')
        for accession, sample_name in sorted(self.name_dict.items()):
            # Create the system call to fastq-dump
            cmd = 'fasterq-dump -e {threads} -O {outdir} -t {outdir} {accession}' \
                    .format(threads=self.threads,
                            outdir=self.path,
                            accession=accession)
            # Only download the files if neither the uncompressed download, or the pigz-compressed file does not exist
            output_prefix = os.path.join(self.path, '{accession}'.format(accession=accession))
            forward_download = '{accession}_1.fastq'.format(accession=output_prefix)
            reverse_download = '{accession}_2.fastq'.format(accession=output_prefix)
            forward_compressed = '{accession}_1.fastq.gz'.format(accession=output_prefix)
            reverse_compressed = '{accession}_2.fastq.gz'.format(accession=output_prefix)
            forward_final = '{sample_name}_R1.fastq.gz'.format(sample_name=os.path.join(self.path, sample_name))
            reverse_final = '{sample_name}_R2.fastq.gz'.format(sample_name=os.path.join(self.path, sample_name))
            if (not os.path.isfile(forward_download) and not os.path.isfile(forward_compressed) and not
                os.path.isfile(forward_final)) and (not os.path.isfile(reverse_download) and not
                os.path.isfile(reverse_compressed) and not os.path.isfile(reverse_final)):
                logging.info('Downloading FASTQ files for {ac}/{sn} from SRA'
                             .format(ac=accession,
                                     sn=sample_name))
                # call(cmd, shell=True)
                os.system(cmd)
            # Use pigz to compress the files in place
            if not os.path.isfile(forward_compressed) and not os.path.isfile(reverse_compressed) and not \
                    os.path.isfile(forward_final) and not os.path.isfile(reverse_final):
                logging.info('Compressing and renaming FASTQ files for {ac}/{sn}'
                             .format(ac=accession,
                                     sn=sample_name))
                pigz_cmd = 'pigz {accession}*'.format(accession=output_prefix)
                call(pigz_cmd, shell=True)
            # Rename the files to have _R1 and _R2 instead of _1 and _2, respectively
            if os.path.isfile(forward_compressed) and not os.path.isfile(forward_final):
                shutil.move(src=forward_compressed,
                            dst=forward_final)
            if os.path.isfile(reverse_compressed) and not os.path.isfile(reverse_final):
                shutil.move(src=reverse_compressed,
                            dst=reverse_final)

    def __init__(self, path, runinfotable, column_name, threads):
        if path.startswith('~'):
            self.path = os.path.abspath(os.path.expanduser(os.path.join(path)))
        else:
            self.path = os.path.abspath(os.path.join(path))
        self.runinfotable = os.path.join(self.path, runinfotable)
        assert os.path.isfile(self.runinfotable), 'Cannot find supplied SRA run info table {rit} in supplied path ' \
                                                  '{sp}'.format(rit=self.runinfotable,
                                                                sp=self.path)
        self.column_name = column_name
        self.name_dict = dict()
        self.threads = threads
        logging.info('Starting SRA download using {at}'.format(at=self.runinfotable))


def cli():
    # Parser for arguments
    parser = ArgumentParser(description='Downloads and compresses FASTQ files from SRA')
    parser.add_argument('-p', '--path',
                        required=True,
                        help='Path to folder containing necessary tables')
    parser.add_argument('-r', '--runinfotable',
                        default='SraRunInfo.csv',
                        help='Name of SRA accession table from NCBI (must be in the supplied path). Generate the table '
                             'from NCBI SRA '
                             'e.g. https://www.ncbi.nlm.nih.gov/sra?LinkName=bioproject_sra_all&from_uid=309770 '
                             'Select Send to: -> File -> RunInfo. Default is SraRunInfo.csv')
    parser.add_argument('-n', '--name',
                        choices=['Run', 'LibraryName', 'Sample', 'BioSample', 'SampleName'],
                        default='SampleName',
                        help='Column name to use for the final naming of the FASTQ files. Default is SampleName')
    parser.add_argument('-t', '--threads',
                        default=multiprocessing.cpu_count() - 1,
                        help='Number of threads. Default is the number of cores in the system minus one')
    arguments = parser.parse_args()
    SetupLogging()
    download = SRAdownload(path=arguments.path,
                           runinfotable=arguments.runinfotable,
                           column_name=arguments.name,
                           threads=arguments.threads)
    download.main()
    logging.info('SRA download complete!')


if __name__ == '__main__':
    cli()
