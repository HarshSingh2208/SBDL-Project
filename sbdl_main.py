import sys
from lib import Utils


from lib.logger import Log4j

if __name__=="__main__":

    if len(sys.argv) < 3:
        print("Usages: sbdl {local qa prod} {load_date} :Arguments are missing")
        sys.exit(-1)

    run_time_env=sys.argv[1].upper()
    load_date=sys.argv[2]
    spark=Utils.get_spark_session(run_time_env)

    logger=Log4j(spark)
    logger.info("Finished creating Spark Session")
    print(spark.version)


