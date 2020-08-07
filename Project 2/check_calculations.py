import configparser
import psycopg2

def calculations(cur,conn):
    """Operates the calculations described within
    Queries defined in sql_queries.py file"""
    
    calc_queries = ["""SELECT a.name AS name, COUNT(*) AS song_count FROM artists AS a JOIN songs AS b ON a.artist_id=b.artist_id GROUP BY a.name"""]

    for q in calc_queries:
        print(q)
        try:
            cur.execute(q)
            
        except Exception as e:
            print("There was an error")
            print(e)
            break
            
        r = cur.fetchmany(20)
        for row in r:
            print(row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    calculations(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()