
import psycopg2
from psycopg2.extras import DictCursor


kwargs = dict(
    dbname='room_n_board',
    host='ncc-1701-a.c5h6quudb39v.us-east-1.rds.amazonaws.com',
    user='',        # Put creds between the
    password='',    # single quotes please.
    port='5432',
)


def query():
    with psycopg2.connect(**kwargs) as conn:
        with conn.cursor(cursor_factory=DictCursor) as curse:
            curse.execute('SELECT * FROM room_n_total_counts')
            for r in curse:
                print(dict(r))


if __name__ == '__main__':

    while True:
        query()
