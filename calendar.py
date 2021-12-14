import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb

list_national_holiday = [
    "01-01", "03-01", "07-17", "08-15", "10-01", "10-03", "12-25"
]

def transaction_data_df(df_base, tbl_nm, replace_yn="N"):

    engine = create_engine("mysql+mysqldb://etlers:"+"wndyd"+"@3.36.123.255/quant", encoding='utf-8')
    conn = engine.connect()

    if replace_yn == "Y":
        df_base.to_sql(name=tbl_nm, con=engine, if_exists='replace', index=False)
    else:
        df_base.to_sql(name=tbl_nm, con=engine, if_exists='append', index=False)

dt_range = pd.date_range(start='20211101', end='20221231')
dt_list = dt_range.strftime("%Y-%m-%d").to_list()
list_holiday = []
for dt in dt_list:
    dt_val = datetime.strptime(dt, '%Y-%m-%d')
    if dt_val.weekday() in [5, 6]:
        holi_yn = "Y"
    elif dt[5:] in list_national_holiday:
        holi_yn = "Y"
    else:
        holi_yn = "N"
    list_holiday.append([dt_val, dt, holi_yn, dt_val.weekday()])
list_holiday

df_holiday = pd.DataFrame(list_holiday, columns=["DT", "DT_CHAR", "HOLI_YN", "HOLI_CD"])

transaction_data_df(df_holiday, "holiday")
