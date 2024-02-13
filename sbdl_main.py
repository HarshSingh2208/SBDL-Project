import sys
import uuid
from pyspark.sql.functions import struct,col
from pyspark.sql.functions import to_json

from lib import Utils
from lib import ConfigLoader
from lib import DataLoader
from lib import Transformations
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
    #accounts_df.show(10)
    contract_df = Transformations.get_contract(accounts_df)
    #contract_df.show(10,truncate=False)

    logger.info("Reading SBDL Parties DF")
    parties_df=DataLoader.read_parties(spark,run_time_env,enable_hive,hive_db)
    #parties_df.show(10)
    relations_df = Transformations.get_relations(parties_df)
    #relations_df.show(10,truncate=False)

    logger.info("Reading SBDL Address DF")
    address_df = DataLoader.read_address(spark, run_time_env, enable_hive, hive_db)
    #address_df.show(10)
    relation_address_df = Transformations.get_address(address_df)
    #relation_address_df.show(10,truncate=False)

    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)
    #party_address_df.show(truncate=False)

    logger.info("Join Account and Parties")
    data_df = Transformations.join_contract_party(contract_df, party_address_df)
    #data_df.show(truncate=False)
    #data_df.printSchema()

    logger.info("Apply Header and create Event")
    final_df = Transformations.apply_header(spark, data_df)
    final_df.printSchema()
    logger.info("Preparing to send data to Kafka")
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))
    kafka_kv_df.printSchema()
    kafka_kv_df.show(1,truncate=False)

    api_key = conf["kafka.api_key"]
    api_secret = conf["kafka.api_secret"]
    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()


    logger.info("Finished sending data to Kafka")




