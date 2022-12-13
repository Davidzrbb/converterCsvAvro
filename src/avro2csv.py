import csv
from avro.datafile import DataFileReader
from avro.io import DatumReader


def convert_to_csv(args):
    avro_file_name = args.in_file
    csv_file_name = args.out_file

    reader = DataFileReader(open(avro_file_name, "rb"), DatumReader())
    try:
        with open(csv_file_name, 'w') as csvfile:
            file_writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL,
                                     lineterminator='\n')
            entry = next(reader)
            header = [str(key) for key, value in entry.items()]
            values = [str(value) for key, value in entry.items()]
            file_writer.writerow(header)
            file_writer.writerow(values)
            for entry in reader:
                row = [str(value) for key, value in entry.items()]
                file_writer.writerow(row)
    finally:
        reader.close()
