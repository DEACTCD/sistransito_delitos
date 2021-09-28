import utils
import sql

base = utils.BaseSistransito()

df = base.get_data()

support = utils.SupportRules(df)

support.set_new_values()
support.set_new_columns()
finish = support.order_columns()
support.excel_df()

send = sql.BancoSistransito()
send.def_temp_table()
send.set_temp_data(finish)
send.merge_sistransito()