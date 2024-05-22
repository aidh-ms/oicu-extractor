"""
TBD
"""
# create_test_db.py
import sqlite3
import yaml
from icu_pipeline.logger import ICULogger

# add logging
logger = ICULogger().get_logger()
logger.setLevel("INFO")
# remove file handler, log only to console
logger.removeHandler(logger.handlers[1])


def load_example_data():
    # load the example yaml file from conceptbase
    with open('conceptbase/example.yml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        logger.info(f"Loaded example data from example.yaml: \n {data}")
        return data


def create_test_mimic(data):
    conn = sqlite3.connect('test_mimiciv.sqlite')
    cursor = conn.cursor()
    cursor.execute('''
                   DROP TABLE IF EXISTS chartevents;''')
    cursor.execute('''
                   CREATE TABLE chartevents (
                          subject_id INT,
                          hadm_id INT,
                          stay_id INT,
                          charttime TEXT,
                          storetime TEXT,
                          itemid INT,
                          value TEXT,
                          valuenum REAL,
                          valueuom TEXT,
                          warning TEXT
                     );
                     ''')
    logger.info("Created chartevents table")
    cursor.execute('''
                   INSERT INTO chartevents (subject_id, hadm_id, stay_id, charttime, storetime, itemid, value, valuenum, valueuom, warning)

                    VALUES

                    (1, 1, 1, '2021-01-01 00:00:00', '2021-01-01 00:00:00', 1, '10', 10, 'bpm', 'normal'),

                    (2, 2, 2, '2021-01-01 00:00:00', '2021-01-01 00:00:00', 2, '20', 20, 'bpm', 'normal'),

                    (3, 3, 3, '2021-01-01 00:00:00', '2021-01-01 00:00:00', 3, '30', 30, 'bpm', 'normal');

                    ''')
    logger.info("Inserted data into chartevents table")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    example_data = load_example_data()
    create_test_mimic(example_data)
