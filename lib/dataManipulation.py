from pyspark.sql.functions import *

def first_score(loans, loan_repayments):
    last_payment_dataframe = loans.join(loan_repayments, 'loan_id', "inner")
    return last_payment_dataframe.show()









    # last_payment_dataframe = spark.sql("""
    #     select l.member_id,
    #     CASE
    #     WHEN lr.last_payment_amount < (l.installement*0.5) THEN {poor_rate}
    #     WHEN (lr.last_payment_amount > (l.installement*0.5) and lr.last_payment_amount < (l.installement))  THEN ${good_rate}
    #     WHEN lr.last_payment_amount > (l.installement) THEN ${very_good_rate}
    #     WHEN lr.last_payment_amount < (l.installement*1.5) THEN ${excellent_rate}
    #     ELSE ${unacceptable_rate}
    #     END AS last_payment_pts,
    #     CASE
    #     WHEN lr.total_payment < (l.funded_amount*0.5) THEN ${good_rate}
    #     WHEN lr.total_payment > (l.funded_amount*0.5) THEN ${excellent_rate}
    #     WHEN lr.total_payment between (l.funded_amount*0.5) and (l.funded_amount) THEN ${very_good_rate}
    #     WHEN lr.total_payment between (l.funded_amount*0.1) and (l.funded_amount*0.5) THEN ${poor_rate}
    #     ELSE ${unacceptable_rate}
    #     END AS total_payment_pts
    #     from itv008842_lending_club.loan_repayments lr inner join itv008842_lending_club.loans l 
    #     on lr.loan_id = l.loan_id and l.member_id not in (select member_id from bad_data)
    #     """).format(poor_rate=conf["spark.sql.poor"], good_rate=conf["spark.sql.good"],
    #     very_good_rate=conf["spark.sql.very_good"], excellent_rate=conf["spark.sql.excellent"],
    #     unacceptable_rate=conf["spark.sql.unacceptable"])