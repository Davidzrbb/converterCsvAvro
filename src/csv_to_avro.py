import csv
from datetime import datetime

import fastavro


def create_aray_data(csv_file_name):
    data = []
    with open(csv_file_name, 'r') as opened_in_file:
        reader = csv.DictReader(opened_in_file, dialect="excel")
        for line in reader:
            for key, value in line.items():
                array_key = key.split(";")
                array_value = value.split(";")
                all_obj = []
                for i in range(len(array_key)):
                    if array_value[i].isdecimal():
                        obj = {array_key[i]: int(array_value[i])}
                    elif array_value[i].replace('.', '', 1).isdigit():
                        obj = {array_key[i]: float(array_value[i])}
                    else:
                        obj = {array_key[i]: array_value[i]}
                    all_obj.append(obj)
                data.append(all_obj)
    return data


def create_fields(data):
    nb_col = len(data[0]) - 1
    fields = []
    list_data = ()
    for i in range(nb_col):
        list_data += (data[0][i + 1].keys(),)
    for col in range(nb_col):
        if data[0][col + 1][list(list_data[col])[0]].__class__.__name__ == 'int':
            fields.append({'name': list(list_data[col])[0], 'type': 'int'})
        elif data[0][col + 1][list(list_data[col])[0]].__class__.__name__ == 'float':
            fields.append({'name': list(list_data[col])[0], 'type': 'float'})
        if data[0][col + 1][list(list_data[col])[0]].__class__.__name__ == 'str':
            try:
                res = bool(datetime.strptime(str(data[0][col + 1][list(list_data[col])[0]]), '%Y-%m-%d %H:%M:%S'))
            except ValueError:
                res = False
            if not res:
                fields.append({'name': list(list_data[col])[0], 'type': 'string'})
            else:
                fields.append(
                    {'name': list(list_data[col])[0],
                     'type': ['null', {'type': 'string', 'logicalType': 'timestamp-millis'}]})
    return fields, list_data, nb_col


def data_dict_to_data_array(data, list_data, nb_col):
    data_array = []
    for col in range(len(data)):
        obj = {}
        for j in range(nb_col):
            obj[list(list_data[j])[0]] = data[col][j + 1][list(list_data[j])[0]]
        data_array.append(obj)
    return data_array


def convert_to_avro(args):
    avro_file_name = args.out_file
    csv_file_name = args.in_file
    data = create_aray_data(csv_file_name)
    fields, list_data, nb_col = create_fields(data)
    schema = {
        "type": "record",
        "namespace": "com.badassmoviecharacters",
        "name": list(data[0][0].keys())[0],
        "doc": "Seriously badass characters",
        "fields": fields
    }
    data_array = data_dict_to_data_array(data, list_data, nb_col)
    # Ouverture d'un fichier binaire en mode écriture
    with open(avro_file_name, 'wb') as avro_file:
        # Ecriture des données
        fastavro.writer(avro_file, schema, data_array)
