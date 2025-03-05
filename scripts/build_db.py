import os
import psycopg2 as psql

file = os.path.join("secrets", ".psql.pass")
with open(file, "r", encoding="UTF-8") as file:
    password=file.read().rstrip()

conn_string=f"host=hadoop-04.uni.innopolis.ru port=5432 user=team36 \
    dbname=team36_projectdb password={password}"

with psql.connect(conn_string) as conn:
    cur = conn.cursor()
    with open(os.path.join("sql","create_table.sql"), encoding="UTF-8") as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    with open(os.path.join("sql", "import_data.sql"), encoding="UTF-8") as file:
        commands= file.readlines()
        with open(os.path.join("data", "accidents.csv"), "r", encoding="UTF-8") as depts:
            cur.copy_expert(commands[0], depts)

    conn.commit()
