import argparse

from avro2csv import convert_to_csv
from csv_to_avro import convert_to_avro

parser = argparse.ArgumentParser(description='Convert CSV to Avro')
parser.add_argument('in_file')
parser.add_argument('out_file')
args = parser.parse_args()


def choice_program(args):
    if args.in_file.endswith('.csv'):
        return convert_to_avro(args)
    else:
        return convert_to_csv(args)


def main():
    choice_program(args)


if __name__ == '__main__':
    main()
