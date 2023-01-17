import os
import warnings

import pyspark.pandas as ps
from pyspark import SparkConf, SparkContext

# conf = SparkConf()
# conf.set('spark.driver.memory', '1g')
# conf.set('spark.executor.memory', '3g')
# SparkContext(conf=conf)

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
warnings.filterwarnings(action='once')

# relative path to the raw data files
file_path = '../../data/raw/Loan_status_2007-2020Q3.gzip'
data_dict_path = '../../data/raw/LCDataDictionary.xlsx'
parquet_output = '../../data/interim/parquet/'


# read in the raw data using
pdf = ps.read_csv(path=file_path, header='infer', dtype='str', index_col='id')\
    .drop('_c0', axis=1)


# correct for the misaligned row in the dataset
pdf1 = pdf[pdf.annual_inc != 'MORTGAGE']
pdf2 = pdf[pdf.annual_inc == 'MORTGAGE']
pdf2a = pdf2.loc[:, :'sub_grade']
emp_t = pdf2.T.loc['emp_title']
pdf2b = pdf2.loc[:, 'emp_title':].T.shift(-1).T
emp_t = emp_t + pdf2b.T.loc['emp_title']
pdf2 = pdf2a.join(pdf2b, how='left')
pdf = pdf1.append(pdf2)

# Classifying columns by dtypes
col_strs = ['grade', 'sub_grade', 'emp_title', 'emp_length', 'home_ownership',
            'verification_status', 'loan_status', 'pymnt_plan', 'url',
            'purpose', 'title', 'zip_code', 'addr_state',
            'initial_list_status', 'application_type',
            'verification_status_joint', 'hardship_flag', 'hardship_type',
            'hardship_reason', 'hardship_status', 'hardship_loan_status',
            'debt_settlement_flag',
           ]

col_dts = ['issue_d', 'earliest_cr_line', 'last_pymnt_d', 'next_pymnt_d',
           'last_credit_pull_d', 'sec_app_earliest_cr_line',
           'hardship_start_date', 'hardship_end_date',
           'payment_plan_start_date',
          ]

col_oth = ['int_rate', 'revol_util', 'sec_app_revol_util', 'term', ]

col_num = [col for col in pdf.columns if col not in col_strs+col_dts+col_oth]


# Converting dataframe columns into respective dtypes
pdf[col_num] = pdf[col_num].astype('float64')

# converting percentage strings
pdf['int_rate'] = pdf.int_rate.str.strip().str.strip('%')\
                      .astype('float64')/100
pdf['revol_util'] = pdf.revol_util.str.strip().str.strip('%')\
                        .astype('float64')/100
pdf['sec_app_revol_util'] = pdf.sec_app_revol_util.str.strip().str.strip('%')\
                                .astype('float64')/100

# Converting term suffixes
pdf['term'] = pdf.term.str.rstrip('months').str.strip().astype('float64')

pdf.to_parquet(path=parquet_output, mode='w',
               compression='gzip', index_col='id',
               partition_cols='sub_grade')
