import pandas as pd
import psycopg2
import os
from datetime import datetime
from airflow.exceptions import AirflowException

def get_db_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("DWH_HOST"),
            database=os.getenv("DWH_NAME"),
            user=os.getenv("DWH_USER"),
            password=os.getenv("DWH_PASSWORD"),
            port=os.getenv("DWH_PORT"),
            options="-c search_path=rd,logs,public"
        )
    except Exception as e:
        raise AirflowException(f"Ошибка подключения к DWH: {e}")

def log_to_db(process_name, status, rows_processed=0, error_message=None):
    start_time = datetime.now()
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO logs.etl_logs 
                    (process_name, start_time, end_time, status, rows_processed, error_message, duration)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    process_name,
                    start_time,
                    datetime.now(),
                    status,
                    rows_processed,
                    error_message,
                    datetime.now() - start_time
                ))
                conn.commit()
    except Exception as e:
        print(f"!!! Ошибка логирования: {e}")

def _read_csv_file(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Файл не найден: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        content = f.read().splitlines()
    
    headers = [h.strip() for h in content[0].split(',')]
    data = []
    for line in content[1:]:
        values = [v.strip() for v in line.split(',')]
        if len(values) == len(headers):
            data.append(values)
    return headers, data

def _prepare_dataframe(headers, data, date_columns=None, default_end_date='2999-12-31'):
    df = pd.DataFrame(data, columns=headers)
    
    if 'effective_to_date' in df.columns:
        df['effective_to_date'] = pd.to_datetime(
            df['effective_to_date'],
            format='%Y-%m-%d',
            errors='coerce'
        ).fillna(pd.Timestamp(default_end_date))
    
    if date_columns:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df

def _load_to_db(df, table_name, schema, special_handling=None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                ALTER TABLE {schema}.{table_name} 
                ALTER COLUMN effective_to_date DROP NOT NULL
            """)
            
            cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
            
            if special_handling == 'product':
                query = f"""
                    INSERT INTO {schema}.{table_name} 
                    (product_rk, product_name, effective_from_date, effective_to_date)
                    VALUES (%s, %s, %s, %s)
                """
                for _, row in df.iterrows():
                    cursor.execute(query, (
                        row['product_rk'],
                        row['product_name'],
                        row['effective_from_date'],
                        row['effective_to_date'] if pd.notna(row['effective_to_date']) else None
                    ))
            else:
                cols = ", ".join([f'"{c}"' for c in df.columns])
                placeholders = ", ".join(["%s"] * len(df.columns))
                query = f"INSERT INTO {schema}.{table_name} ({cols}) VALUES ({placeholders})"
                
                for _, row in df.iterrows():
                    cursor.execute(query, tuple(
                        None if pd.isna(val) else val for val in row
                    ))
            
            cursor.execute(f"""
                ALTER TABLE {schema}.{table_name} 
                ALTER COLUMN effective_to_date SET NOT NULL
            """)
            conn.commit()

def load_csv_smart(csv_path, table_name, schema="rd", date_columns=None):
    process_name = f"LOAD_{schema}.{table_name}"
    log_to_db(process_name, "STARTED")
    
    try:
        headers, data = _read_csv_file(csv_path)
        df = _prepare_dataframe(headers, data, date_columns=['deal_start_date', 'effective_from_date'])
        _load_to_db(df, table_name, schema)
        
        log_to_db(process_name, "SUCCESS", len(df))
        return True
    except Exception as e:
        error_msg = f"Ошибка загрузки: {str(e)}"
        log_to_db(process_name, "FAILED", 0, error_msg)
        raise AirflowException(error_msg)

def load_product_info(csv_path, table_name="product", schema="rd"):
    process_name = f"LOAD_{schema}.{table_name}"
    log_to_db(process_name, "STARTED")
    
    try:
        headers, data = _read_csv_file(csv_path)
        df = _prepare_dataframe(headers, data)
        
        df['product_rk'] = pd.to_numeric(df['product_rk'], errors='coerce')
        df['effective_from_date'] = pd.to_datetime(df['effective_from_date'], errors='coerce')
        
        _load_to_db(df, table_name, schema, special_handling='product')
        
        log_to_db(process_name, "SUCCESS", len(df))
        return True
    except Exception as e:
        error_msg = f"Ошибка загрузки product_info: {str(e)}"
        log_to_db(process_name, "FAILED", 0, error_msg)
        raise AirflowException(error_msg)