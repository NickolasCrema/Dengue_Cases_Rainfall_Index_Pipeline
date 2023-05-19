import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import re

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

dengue_columns = [
                    'id',
                    'data_iniSE',
                    'casos',
                    'ibge_code',
                    'cidade',
                    'uf',
                    'cep',
                    'latitude',
                    'longitude'
]

def list_to_dictionary(element, columns):
    """
    @params:
        element - input list \n
        columns - columns names list
    @return:
        dictionary of list indexed by columns names
    """
    return dict(zip(columns, element))

def text_to_list(element, delimiter):
    """
    @params:
        element - input text \n
        delimiter - character delimiter
    @return:
        list of elements separated by delimiter
    """
    return element.split(delimiter)

def transform_dates(element):
    """
    @params:
        element - data dictionary
    @return:
        data dictionary with YEAR-MONTH column
    """
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element

def uf_key(element):
    """
    @params:
        element - data dictionary
    @return:
        tuple (UF, element)
    """

    key = element['uf']
    return (key, element)

def dengue_cases(element):
    """
    @params:
        element - tuple (UF, data dictionary)
    @return:
        tuple (UF-YEAR-MONTH, number of dengue cases)
    """
    uf, data = element
    for d in data:
        if bool(re.search(r'\d', d.get('casos'))):
            yield (f"{uf}-{d.get('ano_mes')}", float(d.get('casos')))
        else:
            yield (f"{uf}-{d.get('ano_mes')}", 0.0)

def rain_key_uf_year_month(element):
    """
    @params:
        element - list of data
    @return:
        tuple containing key and rainfall (UF-YEAR-MONTH, rainfall)
    """
    date, rainfall, uf = element
    key = f"{uf}-{'-'.join(date.split('-')[:2])}"
    if float(rainfall) >= 0.0:
        return (key, float(rainfall))
    else:
        return (key, 0.0)
    
def round_rainfall(element):
    """
    @params:
        element - data tuple (UF-YEAR-MONTH, rainfall)
    @return:
        data tuple w/ rainfall rounded to 6 precision decimal digits
    """
    key, rainfall = element

    return (key, round(float(rainfall), ndigits=6))
    
def filter_missing_values(element):
    """
    @params:
        element - data tuple (key, data:{})
    @return:
        Bool
    """
    key, data = element
    if all([
        data['rain'],
        data['dengue']
    ]):
        return True
    return False

def unpack_elements(element):
    """
    @params:
        element - data tuple (key, data:{})
    @return
        unpacked data tuple (UF, year, month, rainfall, dengue-cases)
    """
    key, data = element
    uf, year, month = key.split('-')
    return uf, year, month, str(data['rain'][0]), str(data['dengue'][0])

def prepare_csv(element, delimiter):
    """
    @params:
        element - data tuple (uf, year, month, rainfall, dengue-cases)
        delimiter - character delimiter
    @return:
        string joined by delimiter
    """
    return f'{delimiter}'.join(element)

dengue = (
    pipeline
    | "Dengue dataset read" >>
        ReadFromText('dados/casos_dengue.txt', skip_header_lines=1)
    | "Text to list (dengue)" >> beam.Map(text_to_list, '|')
    | "List to dictionary" >> beam.Map(list_to_dictionary, dengue_columns)
    | "Create column YEAR-MONTH" >> beam.Map(transform_dates)
    | "Create UF key" >> beam.Map(uf_key)
    | "Group by state" >> beam.GroupByKey()
    | "Create UF-YEAR-MONTH key (dengue)" >> beam.FlatMap(dengue_cases) # <-- flatmap permite uso de yield
    | "Sum cases by key" >> beam.CombinePerKey(sum)
    # | "print (dengue)" >> beam.Map(print)
)

rain = (
    pipeline
    | "Rain dataset read" >>
        ReadFromText('dados/chuvas.csv', skip_header_lines=1)
    | "Text to list (rain)" >> beam.Map(text_to_list, ',')
    | "Create UF-YEAR-MONTH key (rain)" >> beam.Map(rain_key_uf_year_month)
    | "Sum rainfall by key" >> beam.CombinePerKey(sum)
    | "Round rainfall " >> beam.Map(round_rainfall)
    # | "print (rain)" >> beam.Map(print)
)

rain_dengue = (
    # (rain, dengue)
    # | 'Uniao das pcollections' >> beam.Flatten()
    # | 'Agrupa as pcollections' >> beam.GroupByKey()
    ({'rain': rain, 'dengue': dengue})
    | "Merge pcollections" >> beam.CoGroupByKey()
    | "Filter missing values" >> beam.Filter(filter_missing_values)
    | "Unpack elements" >> beam.Map(unpack_elements)
    | "Prepare csv" >> beam.Map(prepare_csv, ';')
    # | "print (rain_dengue)" >> beam.Map(print)
)
header = 'UF;YEAR;MONTH;RAINFALL;DENGUE_CASES'
rain_dengue | "Persistindo os dados" >> WriteToText('dados/resultado', file_name_suffix='.csv', header=header)

pipeline.run()