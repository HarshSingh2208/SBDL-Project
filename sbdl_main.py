import sys
import uuid
from lib import Utils
from lib import ConfigLoader
from lib import DataLoader


from lib.logger import Log4j

if __name__=="__main__":

    if len(sys.argv) < 3:
        print("Usages: sbdl {local qa prod} {load_date} :Arguments are missing")
        sys.exit(-1)

    run_time_env=sys.argv[1].upper()
    load_date=sys.argv[2]
    job_run_id="SBDL-"+str(uuid.uuid4())
    print('Initailizing in '+run_time_env+" env with Job_id "+job_run_id)

    conf=ConfigLoader.get_config(run_time_env)
    enable_hive= True if conf["enable.hive"]=='true' else False
    hive_db=conf["hive.database"]
    spark = Utils.get_spark_session(run_time_env)
    print(spark.version)
    logger=Log4j(spark)

    logger.info("Reading SBDL Account DF")
    accounts_df=DataLoader.read_accounts(spark,run_time_env,enable_hive,hive_db)
    accounts_df.show(10)

    logger.info("Reading SBDL Parties DF")
    parties_df=DataLoader.read_parties(spark,run_time_env,enable_hive,hive_db)
    parties_df.show(10)

    logger.info("Reading SBDL Address DF")
    address_df = DataLoader.read_address(spark, run_time_env, enable_hive, hive_db)
    address_df.show(10)



