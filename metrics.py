from db_connection import DBConnection
import time

if __name__ == "__main__":
    try:
        with DBConnection() as conn:
            while True:
                time.sleep(5)
                rows = conn.read_time_data()

                sum = 0.0
                diffs = []
                for row in rows:
                    diffs.append(float(row[1]) - float(row[0]))
                    sum = sum + float(row[1]) - float(row[0])

                average_delay = sum/len(rows)
                print(f"Average delay is : {average_delay}")
                print(f"Minumum delay is : {min(diffs)}")
                print(f"Maximum delay is : {max(diffs)}")
    except KeyboardInterrupt:
        pass