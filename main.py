import os, json

# Custom libraries
from loggers import logger
from pipelines import pipedrive_pipelines

log = logger.app_logger()

def get_object(json_file):
    f = open(json_file)
    return json.load(f)

def get_persons(**kwargs):
    log.info('Start getting persons...')
    run_date = kwargs.get('date')
    #log.info(f'Execution Date {run_date}')
    
    persons_schema = get_object('schemas/persons.json')
    _id = persons_schema.get('id')
    log.info(f'Lets see the persons schema: {persons_schema}')
    #persons = pipedrive_pipelines.run_process_persons_daily(run_date,persons_schema)

def main():
	log.info('Hi there! Im running this script')
	day = '2022-09-01'
	persons = get_persons(date=day)

if __name__ == "__main__":
    main()