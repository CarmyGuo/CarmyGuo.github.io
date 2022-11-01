from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import WindPy as wp
import datetime
import pymssql


def fetch_option_info_from_sql(udl_code):
    """获取期权信息"""
    db = pymssql.connect("填入ip", "sa", "123456", "OptionData", charset="cp936")
    sql = "SELECT * FROM [OptionData].[dbo].[Option_Info] WHERE [OptionMarkCode] = '%s' " % (udl_code)
    cursor = db.cursor()
    cursor.execute(sql)

    return cursor.fetchall()


wp.w.start()
option_info_from_wind = \
    wp.w.wset("optioncontractbasicinformation", "exchange=sse;windcode=510050.SH;status=all", usedf=True)[1]

option_info_from_db = pd.DataFrame(fetch_option_info_from_sql("SH510050"),
                                   columns=["OptionCode", "SecName", "OptionMarkCode", "CallOrPut", "ExerciseMode",
                                            "ExercisePrice", "ContractUnit", "ListedDate", "ExpireDate"]
                                   )

option_info_from_wind["OptionCode"] = option_info_from_wind["wind_code"].apply(lambda x: "OP" + x)
option_to_be_inserted = option_info_from_wind.loc[
    ~option_info_from_wind["OptionCode"].isin(option_info_from_db["OptionCode"])].copy()

option_to_be_inserted.rename(
    columns={"sec_name": "SecName", "call_or_put": "CallOrPut", "exercise_price": "ExercisePrice", }, inplace=True)

option_to_be_inserted["OptionMarkCode"] = "SH510050"
option_to_be_inserted["ExerciseMode"] = "欧式"
option_to_be_inserted["ContractUnit"] = 10000.0
option_to_be_inserted["ListedDate"] = option_to_be_inserted["listed_date"].apply(lambda x: x.strftime("%Y%m%d"))
option_to_be_inserted["ExpireDate"] = option_to_be_inserted["expire_date"].apply(lambda x: x.strftime("%Y%m%d"))

option_to_be_inserted.drop(['wind_code', 'trade_code', 'option_mark_code', 'option_type', 'exercise_mode',
                            'contract_unit', 'limit_month', 'listed_date', 'expire_date', 'exercise_date',
                            'settlement_date', 'reference_price', 'settle_mode', 'contract_state'],
                           axis=1, inplace=True)

engine = create_engine("mssql+pymssql://sa:123456@ip地址/%s" % "OptionData")
conn_insert = engine.connect()
option_to_be_inserted.to_sql("Option_Info", conn_insert, if_exists="append", index=False,
                             dtype={"SecName": sqlalchemy.types.VARCHAR(),
                                    "CallOrPut": sqlalchemy.types.VARCHAR(),
                                    "ExercisePrice": sqlalchemy.types.FLOAT(),
                                    "OptionCode": sqlalchemy.types.VARCHAR(),
                                    "OptionMarkCode": sqlalchemy.types.VARCHAR(),
                                    "ExerciseMode": sqlalchemy.types.VARCHAR(),
                                    "ContractUnit": sqlalchemy.types.FLOAT(),
                                    "ListedDate": sqlalchemy.types.VARCHAR(),
                                    "ExpireDate": sqlalchemy.types.VARCHAR()})
