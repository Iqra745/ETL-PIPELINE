import schedule
import time
import subprocess

def job():
    print("Running ETL pipeline...")
    subprocess.run(["python", "etl_pipeline.py"])
    subprocess.run(["python", "load_to_db.py"])

schedule.every().day.at("10:00").do(job)

if __name__ == "__main__":
    print("Scheduler started. Running job daily at 10:00 AM.")
    while True:
        schedule.run_pending()
        time.sleep(60)