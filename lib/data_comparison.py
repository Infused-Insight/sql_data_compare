import logging as log
import pandas as pd

import os
from datetime import datetime
import sqlalchemy
import datacompy

from urllib.parse import quote_plus  # noqa F401


DEFAULT_COMPARISON_PATH = './comparisons/'


def compare_table_data(sql_engine,
                       schema_orig,
                       table_orig,
                       schema_new,
                       table_new,
                       unique_key,
                       path=DEFAULT_COMPARISON_PATH,
                       convert_key_columns=False):
    df_orig, df_new = load_table_data(
        sql_engine,
        schema_orig,
        table_orig,
        schema_new,
        table_new,
        convert_key_columns=convert_key_columns,
    )

    file_name = (
        f'cmp_{schema_orig}_{table_orig}_'
        f'vs_{schema_new}_{table_new}_'
        f'{ datetime.now().strftime("%Y-%m-%d_%H:%M:%S") }'
    )
    comparison = compare_df(df_orig, df_new, unique_key, file_name, path)

    full_path = os.path.realpath(os.path.join(path, file_name))

    return {
        'df_orig': df_orig,
        'df_new': df_new,
        'comparison': comparison,
        'report_path': f'{full_path}.txt',
        'sqlite_path': f'{full_path}.sqlite',
    }


def load_table_data(sql_engine,
                    schema_orig,
                    table_orig,
                    schema_new,
                    table_new,
                    convert_key_columns=False):
    log.info(f'Loading orig table data from {schema_orig}.{table_orig}...')
    df_orig = pd.read_sql_table(
        table_orig,
        schema=schema_orig,
        con=sql_engine,
    )
    if convert_key_columns is True:
        convert_key_columns(df_orig)

    log.info(f'Loading new table data from {schema_new}.{table_new}...')
    df_new = pd.read_sql_table(
        table_new,
        schema=schema_new,
        con=sql_engine,
    )
    if convert_key_columns is True:
        convert_key_columns(df_new)

    return (df_orig, df_new)


def convert_key_columns(df):
    '''
    If you use dbt hash surrogate keys that have a binary column type,
    then this will convert them to strins starting with '0x'
    '''
    for col in df.columns:
        if col.endswith('_key'):
            df[col] = df[col].apply(
                lambda x: f'0x{x.hex().upper()}'
            )


def compare_df(df_orig,
               df_new,
               unique_key,
               file_name,
               path=DEFAULT_COMPARISON_PATH):
    log.info('Comparing dataframes...')
    comparison = datacompy.Compare(
        df_orig,
        df_new,
        join_columns=unique_key,
        df1_name='orig',
        df2_name='new',
    )
    comparison.matches(ignore_extra_columns=False)

    report_str = comparison.report()

    # Save textual summary to txt file
    report_path = os.path.join(
        path,
        f'{file_name}.txt'
    )
    log.info(f'Writing report to file {report_path}...')
    with open(report_path, 'w') as report_file:
        report_file.write(report_str)

    # Save differences to sqlite
    sqlite_path = os.path.join(
        path,
        f'{file_name}.sqlite'
    )
    write_datacompy_to_sqlite(comparison, sqlite_path, unique_key)

    log.info('Printing report...')
    print(report_str)

    return comparison


def write_datacompy_to_sqlite(comparison, path, unique_key):
    log.info(f'Exporting differences to SQLite DB: {path}')

    engine = sqlalchemy.create_engine(f'sqlite:///{path}', echo=False)
    sqlite_connection = engine.connect()

    df1_cols = list(comparison.df1.columns)
    df1_cols.remove(unique_key)

    for i, col in enumerate(df1_cols, start=1):
        table_name = f'col_diff_{i:02}_{ col.lower().replace(" ", "_") }'
        log.info(
            f'\tExporting column differences for `{col}` to: {table_name}'
        )

        col_diff_df = comparison.sample_mismatch(col, sample_count=900000)
        if len(col_diff_df.index) > 0:
            col_diff_df.to_sql(table_name, sqlite_connection, if_exists='fail')
        else:
            log.info('\t\tNo changes in the column, skipping the export.')

    table_name = 'unique_rows_orig'
    log.info(
        f'\tExporting rows that are only in the orig table to: {table_name}'
    )
    if len(comparison.df1_unq_rows.index) > 0:
        comparison.df1_unq_rows.to_sql(
            table_name,
            sqlite_connection,
            if_exists='fail'
        )
    else:
        log.info('\t\tNo unique rows in orig table, skipping export')

    table_name = 'unique_rows_new'
    log.info(
        f'\tExporting rows that are only in the new table to: {table_name}'
    )
    if len(comparison.df2_unq_rows.index) > 0:
        comparison.df2_unq_rows.to_sql(
            table_name,
            sqlite_connection,
            if_exists='fail'
        )
    else:
        log.info('\t\tNo unique rows in new table, skipping export')

    table_name = 'rows_with_differences'
    log.info(
        f'\tExporting all rows that have differences to: {table_name}'
    )
    rows_with_differences = comparison.all_mismatch(
        simplify_matched=True,
        include_match_col=True,
    )

    rows_with_differences.to_sql(
        table_name,
        sqlite_connection,
        if_exists='fail'
    )

    log.info(f'Finished exporting differences to SQLite DB: {path}')
