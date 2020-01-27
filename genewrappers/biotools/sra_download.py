#!/usr/bin/env python3
from olctools.accessoryFunctions.accessoryFunctions import SetupLogging
from argparse import ArgumentParser
from subprocess import call
import multiprocessing
import logging
import shutil
import os


class SRAdownload(object):

    def main(self):
        with open(self.accessiontable, 'r') as sra_list:
            for accession in sra_list:
                # Remove trailing newline characters
                accession = accession.rstrip()
                # Create the system call to fastq-dump
                cmd = 'fasterq-dump -e {threads} -O {outdir} -t {outdir} {accession}' \
                    .format(threads=self.threads,
                            outdir=self.path,
                            accession=accession)
                # Only download the files if neither the uncompressed download, or the pigz-compressed file does not
                # exist
                output_prefix = os.path.join(self.path, '{accession}'.format(accession=accession))
                forward_download = '{accession}_1.fastq'.format(accession=output_prefix)
                reverse_download = '{accession}_2.fastq'.format(accession=output_prefix)
                forward_compressed = '{accession}_1.fastq.gz'.format(accession=output_prefix)
                reverse_compressed = '{accession}_2.fastq.gz'.format(accession=output_prefix)
                forward_final = '{accession}_R1.fastq.gz'.format(accession=output_prefix)
                reverse_final = '{accession}_R2.fastq.gz'.format(accession=output_prefix)
                if (not os.path.isfile(forward_download) and not os.path.isfile(forward_compressed) and not
                    os.path.isfile(forward_final)) and (not os.path.isfile(reverse_download) and not
                    os.path.isfile(reverse_compressed) and not os.path.isfile(reverse_final)):
                    logging.info('Downloading FASTQ files for {accession} from SRA'.format(accession=accession))
                    # call(cmd, shell=True)
                    os.system(cmd)
                # Use pigz to compress the files in place
                if not os.path.isfile('{accession}_1.fastq.gz'.format(accession=output_prefix)) and not os.path.isfile(
                        '{accession}_2.fastq.gz'.format(accession=output_prefix)):
                    logging.info('Compressing and renaming FASTQ files for {accession}'.format(accession=accession))
                    pigz_cmd = 'pigz {accession}*'.format(accession=output_prefix)
                    call(pigz_cmd, shell=True)
                # Rename the files to have _R1 and _R2 instead of _1 and _2, respectively
                if os.path.isfile(forward_compressed) and not os.path.isfile(forward_final):
                    shutil.move(src=forward_compressed,
                                dst=forward_final)
                if os.path.isfile(reverse_compressed) and not os.path.isfile(reverse_final):
                    shutil.move(src=reverse_compressed,
                                dst=reverse_final)

    def __init__(self, path, accessiontable, threads):
        if path.startswith('~'):
            self.path = os.path.abspath(os.path.expanduser(os.path.join(path)))
        else:
            self.path = os.path.abspath(os.path.join(path))
        self.accessiontable = os.path.join(self.path, accessiontable)
        assert os.path.isfile(self.accessiontable), 'Cannot find supplied SRA accession table {at} in supplied path ' \
                                                    '{sp}'.format(at=self.accessiontable,
                                                                  sp=self.path)
        self.threads = threads
        logging.info('Starting SRA download using {at}'.format(at=self.accessiontable))


def cli():
    # Parser for arguments
    parser = ArgumentParser(description='Downloads and compresses FASTQ files from SRA')
    parser.add_argument('-p', '--path',
                        required=True,
                        help='Path to folder containing necessary tables')
    parser.add_argument('-a', '--accessiontable',
                        default='SraAccList.txt',
                        help='Name of SRA accession table from NCBI (must be in the supplied path). Generate the table '
                             'from NCBI SRA '
                             'e.g. https://www.ncbi.nlm.nih.gov/sra?LinkName=bioproject_sra_all&from_uid=309770 '
                             'Select Send to: -> File -> Accession List. Default is SraAccList.txt')
    parser.add_argument('-n', '--numthreads',
                        default=multiprocessing.cpu_count() - 1,
                        help='Number of threads. Default is the number of cores in the system minus one')
    arguments = parser.parse_args()
    SetupLogging()
    download = SRAdownload(path=arguments.path,
                           accessiontable=arguments.accessiontable,
                           threads=arguments.numthreads)
    download.main()
    logging.info('SRA download complete!')


if __name__ == '__main__':
    cli()
